package session

import "testing"

func TestServerAllowsOnlyOneActiveSessionReservation(t *testing.T) {
	s := &Server{}
	if !s.reserveSession() {
		t.Fatal("first reservation should succeed")
	}
	if s.reserveSession() {
		t.Fatal("second reservation should fail while active")
	}
	s.releaseSession()
	if !s.reserveSession() {
		t.Fatal("reservation after release should succeed")
	}
}
