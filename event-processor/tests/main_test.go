package tests

import (
	"testing"
)

// Voorbeeldfunctie om te testen
func ProcessMessage(msg string) bool {
	return msg != ""
}

// Testfunctie voor ProcessMessage
func TestProcessMessage(t *testing.T) {
	// Test met een geldige string
	result := ProcessMessage("test message")
	if !result {
		t.Errorf("Expected true, got false")
	}

	// Test met een lege string
	result = ProcessMessage("")
	if result {
		t.Errorf("Expected false, got true")
	}
}
