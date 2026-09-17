package tlsutil

import (
        "crypto/ecdsa"
        "crypto/elliptic"
        "crypto/rand"
        "crypto/tls"
        "crypto/x509"
        "encoding/pem"
        "math/big"
        "time"
)

// GenerateSelfSigned returns a tls.Certificate for local demos.
func GenerateSelfSigned() (tls.Certificate, error) {
        key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
        if err != nil {
                return tls.Certificate{}, err
        }
        tmpl := &x509.Certificate{
                SerialNumber: big.NewInt(1),
                NotBefore:    time.Now().Add(-time.Hour),
                NotAfter:     time.Now().Add(24 * time.Hour * 365),
                KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
                ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
                DNSNames:     []string{"localhost"},
        }
        der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
        if err != nil {
                return tls.Certificate{}, err
        }
        certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
        keyDER, err := x509.MarshalECPrivateKey(key)
        if err != nil {
                return tls.Certificate{}, err
        }
        keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
        return tls.X509KeyPair(certPEM, keyPEM)
}
