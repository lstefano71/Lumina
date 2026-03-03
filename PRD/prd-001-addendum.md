This is a very common scenario in internal clusters, service meshes (where mTLS is handled by sidecars like Istio/Linkerd), or legacy "air-gapped" environments.

Here is the **Addendum to the PRD** and the specific technical adjustments required to run **Lumina** in a "Plaintext" (No-SSL) environment while maintaining high performance.

### Critical Technical Context

1. **HTTP/3 (QUIC)** is effectively impossible without TLS. The protocol specification mandates TLS 1.3. We must drop HTTP/3 for this specific scenario.
2. **gRPC** normally expects TLS, but it supports a mode called **`h2c` (HTTP/2 over Cleartext)**. This is part of the HTTP/2 standard (RFC 7540) specifically for non-encrypted connections.
3. **.NET Kestrel** supports this, but it requires explicit configuration because modern defaults aggressively push for HTTPS.

---

## PRD Addendum: "Plaintext / No-TLS Operation"

### 1. Protocols & Transport (Revised)

The Ingestion Layer must support **"Insecure High-Performance"** modes.

* **Primary Protocol:** **HTTP/2 Cleartext (`h2c`)**.
  * Used for high-throughput gRPC (OTLP) and multiplexed JSON batches.
  * **Benefit:** Removes the CPU overhead of TLS handshakes and encryption/decryption, actually *increasing* ingestion throughput in trusted networks.
* **Secondary Protocol:** **HTTP/1.1**.
  * Used for simple clients (e.g., `curl`, simple scripts) that do not support HTTP/2 upgrade semantics.
  * Must be enabled on the same port or a secondary port.

### 2. Implementation Specifications (.NET 10)

#### 2.1. Server-Side (Lumina Backend) configuration

You must explicitly configure Kestrel in `Program.cs` to negotiate HTTP protocols without looking for a certificate.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    // CONFIGURATION FOR NO-TLS ENVIRONMENTS
    options.ListenAnyIP(5000, listenOptions =>
    {
        // Force HTTP/2 over Cleartext (h2c) if possible, 
        // but allow HTTP/1.1 for compatibility.
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2; 
    });
});

var app = builder.Build();
```

*Note: In earlier .NET versions, you sometimes had to force `HttpProtocols.Http2` only for gRPC generic hosts. In .NET 10 (and 8+), `Http1AndHttp2` negotiates correctly for cleartext if the client supports it.*

#### 2.2. Client-Side (OTLP Exporter)

When configuring OpenTelemetry collectors or agents to send data to Lumina, the configuration must explicitly disable security.

**OTLP Configuration:**

```yaml
exporters:
  otlp:
    endpoint: "http://lumina-backend:5000" # Note http:// NOT https://
    tls:
      insecure: true             # CRITICAL: Disables certificate checks
      insecure_skip_verify: true # Extra safety for some client versions
```

#### 2.3. Client-Side (.NET gRPC Client)

If you are writing a C# client to push logs to Lumina:

```csharp
// Allow gRPC to use http:// (unencrypted)
// This switch is often needed in older .NET, but usually default in .NET 10 
// for "http" URIs. Just in case:
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var channel = GrpcChannel.ForAddress("http://lumina-backend:5000", new GrpcChannelOptions
{
    Credentials = ChannelCredentials.Insecure
});
```

### 3. Impact on "Resilient WAL"

The lack of TLS **does not** impact the storage reliability.

* **Corruption Risk:** Without TLS, TCP checksums are your only network protection. If a router is failing and flips a bit *after* the TCP check but *before* application memory, you might get corrupt data.
* **Defense:** The **CRC32 checks** defined in our WAL PRD (Section 4.1.2) become even more critical here. They act as the "Application Layer Integrity Check" that TLS would normally provide.

### 4. Revised PRD Section: Security & Deployment

**6.1. Deployment Modes**
The system supports two distinct operation modes configurable via `appsettings.json`:

1. **Secure Mode (Default):**
    * Requires valid X.509 Certificate.
    * Enables HTTP/3 (QUIC) and HTTPS/gRPC.
    * Rejects non-TLS connections.

2. **Intranet / Off-Grid Mode:**
    * **Certificate Requirement:** None.
    * **Transport:** HTTP/2 Cleartext (`h2c`) and HTTP/1.1.
    * **Performance:** Expected to be ~15-20% faster due to zero encryption overhead.
    * **Warning:** Log data traverses the network in plain text. Recommended only for private VPNS, Service Meshes, or physically isolated networks.

---

### Summary of Changes

1. **Drop HTTP/3** from the "Requirements" list for the No-TLS scenario.
2. **Mandate `h2c` support** in the Ingestion Layer.
3. **No changes** to the WAL or Parquet architecture (CRC logic protects the data regardless of transport).
