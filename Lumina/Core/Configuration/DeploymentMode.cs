namespace Lumina.Core.Configuration;

/// <summary>
/// Deployment mode for the Lumina server.
/// </summary>
public enum DeploymentMode
{
    /// <summary>
    /// Secure mode with HTTPS, HTTP/3, and TLS required.
    /// </summary>
    Secure,
    
    /// <summary>
    /// Intranet mode with HTTP/2 Cleartext (h2c) and HTTP/1.1, no TLS.
    /// Suitable for trusted networks where TLS overhead is undesirable.
    /// </summary>
    Intranet
}