package healthcheck

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

// RunIfRequested performs a local HTTP healthcheck when the process is invoked
// as "<service> healthcheck [url]". It exits the process only for that mode.
func RunIfRequested(defaultURL string) {
	if len(os.Args) < 2 || os.Args[1] != "healthcheck" {
		return
	}

	url := defaultURL
	if len(os.Args) >= 3 {
		url = os.Args[2]
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
		fmt.Fprintf(os.Stderr, "unhealthy status: %d\n", resp.StatusCode)
		os.Exit(1)
	}

	os.Exit(0)
}
