package services

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestUploadLibraryAndRoom(t *testing.T) {
	a, err := NewApp(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(a.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	first, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	first.SetReadDeadline(time.Now().Add(3 * time.Second))
	second.SetReadDeadline(time.Now().Add(3 * time.Second))
	// The first client becomes host, and both clients receive room state.
	var firstRole, secondRole bool
	for i := 0; i < 4; i++ {
		var m Message
		if err := first.ReadJSON(&m); err != nil {
			t.Fatal(err)
		}
		if m.Event == "role" {
			var role struct {
				IsHost bool `json:"isHost"`
			}
			json.Unmarshal(m.Data, &role)
			firstRole = role.IsHost
			break
		}
	}
	for i := 0; i < 4; i++ {
		var m Message
		if err := second.ReadJSON(&m); err != nil {
			t.Fatal(err)
		}
		if m.Event == "role" {
			var role struct {
				IsHost bool `json:"isHost"`
			}
			json.Unmarshal(m.Data, &role)
			secondRole = role.IsHost
			break
		}
	}
	if !firstRole || secondRole {
		t.Fatalf("unexpected roles: first=%v second=%v", firstRole, secondRole)
	}

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("audio", "example.mp3")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("fake audio payload"))
	_ = form.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/upload", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		data, _ := io.ReadAll(res.Body)
		t.Fatalf("upload: %d %s", res.StatusCode, data)
	}
	var uploaded struct {
		URL      string   `json:"url"`
		Metadata Metadata `json:"metadata"`
	}
	if err := json.NewDecoder(res.Body).Decode(&uploaded); err != nil {
		t.Fatal(err)
	}
	if uploaded.Metadata.Title != "example" {
		t.Fatalf("metadata: %+v", uploaded.Metadata)
	}
	media, err := http.Get(server.URL + uploaded.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer media.Body.Close()
	if media.StatusCode != 200 {
		t.Fatalf("media: %d", media.StatusCode)
	}

	saved, err := http.Get(server.URL + "/api/saved")
	if err != nil {
		t.Fatal(err)
	}
	defer saved.Body.Close()
	var library struct {
		Saved []SavedTrack `json:"saved"`
	}
	if err := json.NewDecoder(saved.Body).Decode(&library); err != nil {
		t.Fatal(err)
	}
	if len(library.Saved) != 1 {
		t.Fatalf("saved tracks: %d", len(library.Saved))
	}
	id := library.Saved[0].ID
	like, err := http.Post(server.URL+"/api/saved/"+id+"/like", "application/json", strings.NewReader(`{"name":"Mina"}`))
	if err != nil {
		t.Fatal(err)
	}
	like.Body.Close()
	if like.StatusCode != 200 {
		t.Fatalf("like: %d", like.StatusCode)
	}
	state, err := http.Get(server.URL + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Body.Close()
	var room State
	if err := json.NewDecoder(state.Body).Decode(&room); err != nil {
		t.Fatal(err)
	}
	if room.Track == nil || room.Track.URL != uploaded.URL {
		t.Fatalf("state: %+v", room)
	}
	if err := first.WriteJSON(message("seek", 12.5)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		var m Message
		if err := second.ReadJSON(&m); err != nil {
			t.Fatal(err)
		}
		if m.Event == "seek" {
			var position float64
			json.Unmarshal(m.Data, &position)
			if position != 12.5 {
				t.Fatalf("seek: %v", position)
			}
			return
		}
	}
	t.Fatal("seek event was not broadcast")
}
func TestRejectInvalidUploadAndPreserveExistingData(t *testing.T) {
	dir := t.TempDir()
	existing := `[{"id":"old","filename":"old.mp3","originalName":"Old.mp3","url":"/uploads/old.mp3","savedAt":"2025-01-01T00:00:00Z","metadata":{"title":"Old"}}]`
	if err := os.WriteFile(filepath.Join(dir, "saved-tracks.json"), []byte(existing), 0644); err != nil {
		t.Fatal(err)
	}
	a, err := NewApp(dir)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(a.Handler())
	defer server.Close()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, _ := form.CreateFormFile("audio", "bad.exe")
	_, _ = part.Write([]byte("bad"))
	_ = form.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/upload", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("invalid upload: %d", res.StatusCode)
	}
	saved, err := http.Get(server.URL + "/api/saved")
	if err != nil {
		t.Fatal(err)
	}
	defer saved.Body.Close()
	var library struct {
		Saved []SavedTrack `json:"saved"`
	}
	if err := json.NewDecoder(saved.Body).Decode(&library); err != nil {
		t.Fatal(err)
	}
	if len(library.Saved) != 1 || library.Saved[0].ID != "old" {
		t.Fatalf("existing library lost: %+v", library)
	}
}
