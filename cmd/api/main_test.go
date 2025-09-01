package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInvalidCreateAccount(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/accounts", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	s := &Server{}
	http.HandlerFunc(s.handleCreateAccount).ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
