package client

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type seen struct {
	method string
	path   string
	token  string
	body   []byte
}

func backend(t *testing.T, code int, reply string) (*Client, *seen) {
	t.Helper()
	got := &seen{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method = r.Method
		got.path = r.URL.Path
		got.token = r.Header.Get(tokenHeader)
		got.body, _ = io.ReadAll(r.Body)
		w.WriteHeader(code)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL+"/instances/i1/agent", "tok", "test", slog.New(slog.NewTextHandler(io.Discard, nil))), got
}

func TestCheckpointURLSendsSizeAndDigestOfThePackedArchive(t *testing.T) {
	for _, reply := range []string{
		`{"key":"u1/ck/1","upload_url":"https://b2/put?sig=1"}`,
		`{"status":"success","code":200,"data":{"key":"u1/ck/1","upload_url":"https://b2/put?sig=1"}}`,
	} {
		c, got := backend(t, http.StatusOK, reply)

		target, err := c.CheckpointURL(context.Background(), CheckpointRequest{SizeBytes: 45, SHA256: "abc"})

		if err != nil || target.Key != "u1/ck/1" || target.UploadURL != "https://b2/put?sig=1" {
			t.Fatalf("reply %s: target=%+v err=%v", reply, target, err)
		}
		if got.method != http.MethodPost || got.path != "/instances/i1/agent/state/checkpoint-url" || got.token != "tok" {
			t.Fatalf("request %+v", got)
		}
		var body map[string]any
		if err := json.Unmarshal(got.body, &body); err != nil {
			t.Fatal(err)
		}
		if len(body) != 2 || body["size_bytes"] != float64(45) || body["sha256"] != "abc" {
			t.Fatalf("body %s", got.body)
		}
	}
}

func TestCheckpointURLWithoutUploadURLIsAnError(t *testing.T) {
	c, _ := backend(t, http.StatusOK, `{"key":"u1/ck/1"}`)
	if _, err := c.CheckpointURL(context.Background(), CheckpointRequest{SizeBytes: 1, SHA256: "abc"}); err == nil {
		t.Fatal("reply without upload_url accepted")
	}
}

func TestCheckpointURLStatuses(t *testing.T) {
	for code, want := range map[int]string{http.StatusConflict: "conflict", http.StatusBadRequest: "refused", http.StatusBadGateway: "other"} {
		c, _ := backend(t, code, `{"message":"no"}`)
		_, err := c.CheckpointURL(context.Background(), CheckpointRequest{SizeBytes: 1, SHA256: "abc"})
		got := "other"
		switch {
		case err == nil:
			got = "ok"
		case IsConflict(err):
			got = "conflict"
		case IsBadRequest(err):
			got = "refused"
		}
		if got != want {
			t.Fatalf("http %d: got %s (%v), want %s", code, got, err, want)
		}
	}
}

func TestCommitCheckpointSendsKeyAndDigest(t *testing.T) {
	c, got := backend(t, http.StatusNoContent, "")

	err := c.CommitCheckpoint(context.Background(), CheckpointCommit{Key: "u1/ck/1", SHA256: "abc", SizeBytes: 45})

	if err != nil {
		t.Fatal(err)
	}
	if got.method != http.MethodPost || got.path != "/instances/i1/agent/state/checkpoints" || got.token != "tok" {
		t.Fatalf("request %+v", got)
	}
	var body map[string]any
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 3 || body["key"] != "u1/ck/1" || body["sha256"] != "abc" || body["size_bytes"] != float64(45) {
		t.Fatalf("body %s", got.body)
	}
}

func TestCommitCheckpointStatuses(t *testing.T) {
	for code, conflict := range map[int]bool{http.StatusConflict: true, http.StatusInternalServerError: false} {
		c, _ := backend(t, code, "")
		err := c.CommitCheckpoint(context.Background(), CheckpointCommit{Key: "k", SHA256: "abc", SizeBytes: 1})
		if err == nil || IsConflict(err) != conflict {
			t.Fatalf("http %d: err=%v conflict=%v", code, err, IsConflict(err))
		}
	}
}

func TestStorageCredentialsAreAskedPerDrive(t *testing.T) {
	for _, reply := range []string{
		`{"endpoint":"https://s3.eu-central-003.backblazeb2.com","accessKey":"AK","secretKey":"SK","credentialId":"cid"}`,
		`{"status":"success","code":200,"data":{"endpoint":"https://s3.eu-central-003.backblazeb2.com","accessKey":"AK","secretKey":"SK","credentialId":"cid"}}`,
	} {
		c, got := backend(t, http.StatusOK, reply)

		creds, err := c.GetStorageCredentials(context.Background(), "d-1")

		if err != nil {
			t.Fatalf("reply %s: %v", reply, err)
		}
		want := StorageCredentials{Endpoint: "https://s3.eu-central-003.backblazeb2.com", AccessKey: "AK", SecretKey: "SK", CredentialID: "cid"}
		if *creds != want {
			t.Fatalf("creds %+v", creds)
		}
		if got.method != http.MethodPost || got.path != "/instances/i1/agent/storage/credentials" || got.token != "tok" {
			t.Fatalf("request %+v", got)
		}
		var body map[string]any
		if err := json.Unmarshal(got.body, &body); err != nil {
			t.Fatal(err)
		}
		if len(body) != 1 || body["drive_id"] != "d-1" {
			t.Fatalf("body %s", got.body)
		}
	}
}

func TestStorageCredentialsOfDetachedDriveAreRefused(t *testing.T) {
	c, _ := backend(t, http.StatusBadRequest, `{"message":"drive is not attached"}`)
	_, err := c.GetStorageCredentials(context.Background(), "d-1")
	if !IsBadRequest(err) {
		t.Fatalf("4xx must reach the caller, got %v", err)
	}
}

func TestIncompleteStorageCredentialsAreAnError(t *testing.T) {
	for _, reply := range []string{
		`{"endpoint":"https://s3","secretKey":"SK","credentialId":"cid"}`,
		`{"endpoint":"https://s3","accessKey":"AK","credentialId":"cid"}`,
		`{"accessKey":"AK","secretKey":"SK","credentialId":"cid"}`,
	} {
		c, _ := backend(t, http.StatusOK, reply)
		if _, err := c.GetStorageCredentials(context.Background(), "d-1"); err == nil {
			t.Fatalf("reply %s accepted", reply)
		}
	}
}
