package datastalenessreceiver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func init() { openSchemaRegistry = newSchemaRegistryClient }

type srClient struct {
	base string
	user string
	pass string
	http *http.Client
}

func newSchemaRegistryClient(cfg SourceConfig) (schemaRegistryClient, error) {
	client := &http.Client{}
	if cfg.TLS != nil && *cfg.TLS {
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	}
	return &srClient{
		base: strings.TrimRight(cfg.RegistryURL, "/"),
		user: cfg.Username,
		pass: cfg.Password,
		http: client,
	}, nil
}

// latestVersion returns the highest registered schema version for a subject
// (GET /subjects/{subject}/versions -> [1,2,...]).
func (c *srClient) latestVersion(ctx context.Context, subject string) (int, error) {
	u := fmt.Sprintf("%s/subjects/%s/versions", c.base, url.PathEscape(subject))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("schema registry returned %d for subject %q", resp.StatusCode, subject)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return 0, err
	}
	var versions []int
	if err := json.Unmarshal(body, &versions); err != nil {
		return 0, err
	}
	if len(versions) == 0 {
		return 0, fmt.Errorf("no versions registered for subject %q", subject)
	}
	latest := versions[0]
	for _, v := range versions {
		if v > latest {
			latest = v
		}
	}
	return latest, nil
}

func (c *srClient) close() {}
