package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

func healthCheck(ctx context.Context, listenAddr string) error {
	reqURL := url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort("localhost", strings.TrimPrefix(listenAddr, ":")),
		Path:   "/healthz",
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		return fmt.Errorf("health check failed (%d): %s %v", resp.StatusCode, string(body), err)
	}
	return nil
}
