package jet_stream

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type JetStreamHandler struct {
	Jstream jetstream.JetStream
	Nats    *nats.Conn
}

func CreateJetStreamHandler(ctx context.Context, natUrl string) (*JetStreamHandler, error) {
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	nc, err := nats.Connect(natUrl) //nats.DefaultURL
	if err != nil {
		return nil, fmt.Errorf("Error in connecting NATS %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("Error in creating Jet Stream %w", err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"task.>"},
	})
	return &JetStreamHandler{
		Jstream: js,
		Nats:    nc,
	}, err
}

// func CreateStream(ctx context.Context, streamName string, subject string) error {
// 	_, err := p.Jstream.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
// 		Name:     streamName,
// 		Subjects: []string{subject},
// 	})
// 	if err != nil {
// 		return fmt.Errorf("Error in creating stream %s, Error :%w", streamName, err)
// 	}
// 	return err
// }

func (p *JetStreamHandler) Publish(ctx context.Context, message []byte, subject string) error {
	pAck, err := p.Jstream.Publish(ctx, subject, message)
	if err != nil {
		return fmt.Errorf("Error in publishing message %w", err)
	}
	log.Printf("Message published to %s. Sequence: %d", pAck.Stream, pAck.Sequence)

	return nil
}

func (p *JetStreamHandler) GraceFullShutdown() {
	p.Jstream.Conn().Close()
	p.Nats.Close()
}
