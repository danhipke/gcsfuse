package storageutil

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/auth"
	mountpkg "github.com/googlecloudplatform/gcsfuse/internal/mount"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
)

const CustomEndpoint = "https://localhost:9000"
const DummyKeyFile = "test/test_creds.json"

type StorageClientConfig struct {
	ClientProtocol      mountpkg.ClientProtocol
	MaxConnsPerHost     int
	MaxIdleConnsPerHost int
	HttpClientTimeout   time.Duration
	MaxRetryDuration    time.Duration
	RetryMultiplier     float64
	UserAgent           string
	Endpoint            *url.URL
	KeyFile             string
	TokenUrl            string
	ReuseTokenFromUrl   bool
}

// GetDefaultStorageClientConfig is only for test, making the default endpoint
// non-nil, so that we can create dummy tokenSource while unit test.
func GetDefaultStorageClientConfig() (clientConfig StorageClientConfig) {
	return StorageClientConfig{
		ClientProtocol:      mountpkg.HTTP1,
		MaxConnsPerHost:     10,
		MaxIdleConnsPerHost: 100,
		HttpClientTimeout:   800 * time.Millisecond,
		MaxRetryDuration:    30 * time.Second,
		RetryMultiplier:     2,
		UserAgent:           "gcsfuse/unknown (Go version go1.20-pre3 cl/474093167 +a813be86df) (GCP:gcsfuse)",
		Endpoint:            &url.URL{},
		KeyFile:             DummyKeyFile,
		TokenUrl:            "",
		ReuseTokenFromUrl:   true,
	}
}

func CreateHttpClientObj(storageClientConfig *StorageClientConfig) (httpClient *http.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if storageClientConfig.ClientProtocol == mountpkg.HTTP1 {
		transport = &http.Transport{
			MaxConnsPerHost:     storageClientConfig.MaxConnsPerHost,
			MaxIdleConnsPerHost: storageClientConfig.MaxIdleConnsPerHost,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}
	} else {
		// For http2, change in MaxConnsPerHost doesn't affect the performance.
		transport = &http.Transport{
			DisableKeepAlives: true,
			MaxConnsPerHost:   storageClientConfig.MaxConnsPerHost,
			ForceAttemptHTTP2: true,
		}
	}

	tokenSrc, err := createTokenSource(storageClientConfig)
	if err != nil {
		err = fmt.Errorf("while fetching tokenSource: %w", err)
		return
	}

	// Custom http client for Go Client.
	httpClient = &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: tokenSrc,
		},
		Timeout: storageClientConfig.HttpClientTimeout,
	}

	// Setting UserAgent through RoundTripper middleware
	httpClient.Transport = &userAgentRoundTripper{
		wrapped:   httpClient.Transport,
		UserAgent: storageClientConfig.UserAgent,
	}

	return httpClient, err
}

// IsProdEndpoint -  If user pass any url including (GCS prod) using --endpoint flag
// GCSFuse assumes it as a custom endpoint. Hence, we don't encourage to use --endpoint
// flag to pass actual GCS prod url, as in that case we also need to take care mTLS
// option, which hasn't been handled properly in go-client-lib (by design of WithEndpoint
// option used by GCSFuse).
func IsProdEndpoint(endpoint *url.URL) bool {
	return endpoint == nil
}

func createTokenSource(storageClientConfig *StorageClientConfig) (tokenSrc oauth2.TokenSource, err error) {
	if IsProdEndpoint(storageClientConfig.Endpoint) {
		return auth.GetTokenSource(context.Background(), storageClientConfig.KeyFile, storageClientConfig.TokenUrl, storageClientConfig.ReuseTokenFromUrl)
	} else {
		return oauth2.StaticTokenSource(&oauth2.Token{}), nil
	}
}
