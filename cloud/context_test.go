package cloud

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/studio-b12/gowebdav"
)

func TestCloudOperationContextCancelsMetadataRequests(t *testing.T) {
	for _, provider := range []string{"s3", "webdav", "siyuan"} {
		t.Run(provider, func(t *testing.T) {
			started := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case started <- struct{}{}:
				default:
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			base := &BaseCloud{Conf: &Conf{Dir: "main", Endpoint: server.URL + "/", UserID: "test",
				S3: &ConfS3{Endpoint: server.URL, Region: "us-east-1", Bucket: "notes", AccessKey: "key", SecretKey: "secret", PathStyle: true, Timeout: 10}}}
			var backend Cloud
			switch provider {
			case "s3":
				backend = NewS3(base, server.Client())
			case "webdav":
				backend = NewWebDAV(base, gowebdav.NewClient(server.URL, "", ""))
			case "siyuan":
				backend = NewSiYuan(base)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base.SetContext(ctx)
			done := make(chan error, 1)
			go func() { _, err := backend.DownloadObject("refs/latest"); done <- err }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("metadata request did not start")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled metadata request succeeded")
				}
				if provider == "s3" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation cause lost: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("metadata request ignored cancellation")
			}
			base.SetContext(nil)
			if base.Context().Err() != nil {
				t.Fatal("context leaked into subsequent operations")
			}
		})
	}
}
