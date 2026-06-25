package server

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
)

// edgeVerifier builds a bearer-token verifier for the optional edge-auth mode.
// On success the verified subject becomes the engine user (passed through as the
// X-{Trino,Presto}-User header) while the caller's original token is still
// forwarded to the engine.
func edgeVerifier(ea *config.EdgeAuthConfig) (auth.TokenVerifier, error) {
	switch ea.Scheme {
	case config.EdgeAuthJWTRS256:
		pub, err := loadRSAPublicKey(ea.PublicKeyRef)
		if err != nil {
			return nil, err
		}
		return rs256Verifier(pub), nil
	default:
		return nil, fmt.Errorf("unsupported edge auth scheme %q", ea.Scheme)
	}
}

func rs256Verifier(pub *rsa.PublicKey) auth.TokenVerifier {
	return func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		sub, exp, err := verifyRS256(token, pub)
		if err != nil {
			// The contract: a failure must unwrap to auth.ErrInvalidToken.
			return nil, fmt.Errorf("verify token: %v: %w", err, auth.ErrInvalidToken)
		}
		return &auth.TokenInfo{UserID: sub, Expiration: exp}, nil
	}
}

// loadRSAPublicKey resolves a PEM public-key reference and parses it.
func loadRSAPublicKey(ref string) (*rsa.PublicKey, error) {
	pemStr, err := credential.DefaultResolver(ref)
	if err != nil {
		return nil, fmt.Errorf("public_key_ref: %w", err)
	}
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("public_key_ref: no PEM block found")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("public_key_ref: parse: %w", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public_key_ref: not an RSA public key")
	}
	return rsaPub, nil
}

// verifyRS256 validates an RS256 JWT signature against pub and the expiry claim,
// returning the subject and expiry. It uses only the standard library (no JWT
// dependency), matching the RS256 tokens the engines themselves verify.
func verifyRS256(token string, pub *rsa.PublicKey) (sub string, exp time.Time, err error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", time.Time{}, fmt.Errorf("malformed JWT")
	}

	hdrBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", time.Time{}, fmt.Errorf("header: %w", err)
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(hdrBytes, &hdr); err != nil {
		return "", time.Time{}, fmt.Errorf("header: %w", err)
	}
	if hdr.Alg != "RS256" {
		return "", time.Time{}, fmt.Errorf("unexpected alg %q (want RS256)", hdr.Alg)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", time.Time{}, fmt.Errorf("signature: %w", err)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig); err != nil {
		return "", time.Time{}, fmt.Errorf("signature: %w", err)
	}

	claimBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", time.Time{}, fmt.Errorf("claims: %w", err)
	}
	var claims struct {
		Sub string `json:"sub"`
		Exp int64  `json:"exp"`
	}
	if err := json.Unmarshal(claimBytes, &claims); err != nil {
		return "", time.Time{}, fmt.Errorf("claims: %w", err)
	}
	if claims.Exp != 0 {
		exp = time.Unix(claims.Exp, 0)
		if time.Now().After(exp) {
			return "", time.Time{}, fmt.Errorf("token expired")
		}
	}
	return claims.Sub, exp, nil
}
