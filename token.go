package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
	"github.com/golang-jwt/jwt/v5"
)

// JwtManager manages operations related to JWT tokens
type JwtManager struct {
	publicKeyConfigURL string
	issuerKeyPath      string
	publicKeyConfig    *PublicKeyConfig
	jwks               *Jwks
}

// keys in JWKSResponse
type key struct {
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// Jwks represents the JSON Web Key Set response structure
type Jwks struct {
	Keys []key `json:"keys"`
}

// PublicKeyConfig represents the public key config response structure
type PublicKeyConfig struct {
	Issuer   string `json:"issuer"`
	Jwks_uri string `json:"jwks_uri"`
}

// init initializes the JwtManager with provided values & performs setup tasks
func (jm *JwtManager) init(PublicKeyConfigURL, IssuerKeyPath string) error {
	// Example default values or setup tasks
	jm.publicKeyConfigURL = PublicKeyConfigURL
	jm.issuerKeyPath = IssuerKeyPath

	// Fetch public key configuration
	err := jm._fetchPublicKeyConfig()
	if err != nil {
		return fmt.Errorf("error fetching public key config: %v", err)
	}

	// Fetch JWKS
	err = jm._fetchJWKS()
	if err != nil {
		return fmt.Errorf("error fetching JWKS: %v", err)
	}

	return nil
}

// Retrieves our public key configuration
func (jm *JwtManager) _fetchPublicKeyConfig() error {
	// Perform HTTP GET request
	resp, err := http.Get(jm.publicKeyConfigURL)
	if err != nil {
		return fmt.Errorf("error fetching public key config: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-OK HTTP status: %v", resp.Status)
	}

	// Decode response body into PublicKeyConfigResponse struct
	var config PublicKeyConfig
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return fmt.Errorf("error decoding public key config JSON: %v", err)
	}

	jm.publicKeyConfig = &config

	return nil
}

// Retrieves our SciToken public key
func (jm *JwtManager) _fetchJWKS() error {
	// Perform HTTP GET request
	resp, err := http.Get(jm.publicKeyConfig.Jwks_uri)
	if err != nil {
		return fmt.Errorf("error fetching JWKS: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-OK HTTP status: %v", resp.Status)
	}

	// Decode response body into JWKSResponse struct
	var jwks Jwks
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("error decoding JWKS JSON: %v", err)
	}

	jm.jwks = &jwks

	return nil
}

// Sign generated Jwt token, now you can use `signedToken` in your
// HTTP requests with Authorization header as "Bearer <token>"
func (jm *JwtManager) _signJwtToken(token *jwt.Token) (string, error) {

	// Load private key from file (issuer-key.pem)
	keyData, err := os.ReadFile(jm.issuerKeyPath)
	if err != nil {
		return "", fmt.Errorf("error reading private key file: %v", err)
	}

	// Parse the private key
	key, err := jwt.ParseECPrivateKeyFromPEM(keyData)
	if err != nil {
		return "", fmt.Errorf("error parsing private key: %v", err)
	}

	// Sign the token with private key and get the complete signed token string
	signedToken, err := token.SignedString(key)
	if err != nil {
		return "", fmt.Errorf("error signing token: %v", err)
	}

	fmt.Println("Generated JWT token:", signedToken)

	return signedToken, nil
}

// Creates a new signed jwt Token with the specified Claims
func (jm *JwtManager) generateJwtToken(keyID *string) (string, error) {
	var selectedKey *key
	found := false
	for _, k := range jm.jwks.Keys {
		if k.Kid == *keyID {
			selectedKey = &k
			found = true
			break
		}
	}
	if !found {
		return "", fmt.Errorf("key with key_id %s not found in JWKS", *keyID)
	}

	// Claims for the JWT
	claims := jwt.MapClaims{
		"ver":    "scitoken:2.0",
		"sub":    "test",
		"aud":    "ANY",
		"scope":  "read:/test-write.txt write:/test-write.txt",
		"iat":    time.Now().Unix(),                // Issued At (current time)
		"exp":    time.Now().Add(time.Hour).Unix(), // Expire time (1 hour from now)
		"iss":    jm.publicKeyConfig.Issuer,
		"key_id": selectedKey.Kid,
	}

	// Determine signing method based on alg
	var signingMethod jwt.SigningMethod
	switch selectedKey.Alg {
	case "ES256":
		signingMethod = jwt.SigningMethodES256
	case "HS256":
		signingMethod = jwt.SigningMethodHS256
	case "PS256":
		signingMethod = jwt.SigningMethodPS256
	case "RS256":
		signingMethod = jwt.SigningMethodRS256
	default:
		return "", fmt.Errorf("unsupported signing algorithm: %s", selectedKey.Alg)
	}

	// Create a new token
	token := jwt.NewWithClaims(signingMethod, claims)

	//sign token
	signedToken, err := jm._signJwtToken(token)
	if err != nil {
		return "", fmt.Errorf("error signing token: %v", err)
	}

	return signedToken, nil
}
