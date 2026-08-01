package redis

import (
	"testing"
)

func TestDecodeErrorReturnsGoError(t *testing.T) {
	value, err := Decode([]byte("-ERR unknown command\r\n"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if value != nil {
		t.Fatalf("expected nil value for error response, got %#v", value)
	}
	if err.Error() != "ERR unknown command" {
		t.Fatalf("unexpected error message: %q", err.Error())
	}
}
