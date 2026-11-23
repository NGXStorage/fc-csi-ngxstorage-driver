package helper

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestFCStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := FCVolumeState{SCSIID: "589c-test", MapName: "mpathc", PathNames: []string{"sde", "sdf"}}
	if err := SaveFCState(dir, "volume-1", want); err != nil {
		t.Fatalf("SaveFCState: %v", err)
	}
	got, err := LoadFCState(dir, "volume-1")
	if err != nil {
		t.Fatalf("LoadFCState: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
	if err := DeleteFCState(dir, "volume-1"); err != nil {
		t.Fatalf("DeleteFCState: %v", err)
	}
	if err := DeleteFCState(dir, "volume-1"); err != nil {
		t.Fatalf("idempotent DeleteFCState: %v", err)
	}
}

func TestFCStateConcurrentWritersLeaveOneCompleteRecord(t *testing.T) {
	dir := t.TempDir()
	var writers sync.WaitGroup
	errors := make(chan error, 24)
	for i := 0; i < 24; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			errors <- SaveFCState(dir, "same-volume", FCVolumeState{
				SCSIID: fmt.Sprintf("scsi-%d", i), MapName: "map", PathNames: []string{"sda"},
			})
		}(i)
	}
	writers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	state, err := LoadFCState(dir, "same-volume")
	if err != nil || state.MapName != "map" || len(state.PathNames) != 1 || state.PathNames[0] != "sda" {
		t.Fatalf("incomplete final FC state: %+v %v", state, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary files leaked: %v %v", entries, err)
	}
}
