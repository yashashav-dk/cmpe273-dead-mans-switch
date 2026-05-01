package detector

import "testing"

func TestStateString(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{Alive, "ALIVE"},
		{Missing, "MISSING"},
		{Dead, "DEAD"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}

func TestStateParse(t *testing.T) {
	for _, name := range []string{"ALIVE", "MISSING", "DEAD"} {
		s, err := ParseState(name)
		if err != nil {
			t.Fatalf("ParseState(%q) error: %v", name, err)
		}
		if s.String() != name {
			t.Errorf("roundtrip mismatch: %q -> %v -> %q", name, s, s.String())
		}
	}
	if _, err := ParseState("nope"); err == nil {
		t.Error("ParseState(\"nope\") expected error, got nil")
	}
}
