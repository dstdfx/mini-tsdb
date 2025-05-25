package storage

import (
	"reflect"
	"testing"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
)

func TestInMemoryEfficient_BuildHash(t *testing.T) {
	s := NewInMemoryEfficient()

	labels := []domain.Label{
		{
			Name:  "test",
			Value: "123",
		},
		{
			Name:  "namespace",
			Value: "jobs",
		},
		{
			Name:  "state",
			Value: "stable",
		},
		{
			Name:  "abc",
			Value: "hello",
		},
	}

	got := s.buildLabelsHash(labels)
	expected := LabelsHash(uint64(8341061335512845696))

	if got != expected {
		t.Errorf("expected '%v' but got '%v'", expected, got)
		t.Fail()
	}
}

func TestInMemoryEfficient_Write_Read(t *testing.T) {
	s := NewInMemoryEfficient()

	labels := []domain.Label{
		{
			Name:  "test",
			Value: "123",
		},
		{
			Name:  "namespace",
			Value: "jobs",
		},
		{
			Name:  "state",
			Value: "stable",
		},
		{
			Name:  "abc",
			Value: "hello",
		},
	}

	tNow := time.Now().Unix()

	samples := []domain.Sample{
		{
			Timestamp: tNow,
			Value:     123,
		},
		{
			Timestamp: tNow + 1,
			Value:     124,
		},
		{
			Timestamp: tNow + 2,
			Value:     125,
		},
	}

	s.Write(labels, samples)

	got, err := s.Read(labels)
	if err != nil {
		t.Errorf("unexpected error: '%v'", err)
		t.Fail()
	}

	if !reflect.DeepEqual(got, samples) {
		t.Errorf("expected values to be equal: '%v' to '%v'", got, samples)
	}
}
