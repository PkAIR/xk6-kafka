package kafka

import (
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/grafana/sobek"
	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/v2/js/common"
)

type ConnectionConfig struct {
	Address string     `json:"address"`
	SASL    SASLConfig `json:"sasl"`
	TLS     TLSConfig  `json:"tls"`
}

// connectionClass is a constructor for the Connection object in JS
// that creates a new connection for creating, listing and deleting topics,
// e.g. new Connection(...).
// nolint: funlen
func (k *Kafka) connectionClass(call sobek.ConstructorCall) *sobek.Object {
	runtime := k.vu.Runtime()
	var connectionConfig *ConnectionConfig
	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
	}

	if params, ok := call.Argument(0).Export().(map[string]any); ok {
		if b, err := json.Marshal(params); err != nil {
			common.Throw(runtime, err)
		} else {
			if err = json.Unmarshal(b, &connectionConfig); err != nil {
				common.Throw(runtime, err)
			}
		}
	}

	connection := k.getKafkaControllerConnection(connectionConfig)

	connectionObject := runtime.NewObject()
	// This is the connection object itself
	if err := connectionObject.Set("This", connection); err != nil {
		common.Throw(runtime, err)
	}

	err := connectionObject.Set("createTopic", func(call sobek.FunctionCall) sobek.Value {
		var topicConfig *kafkago.TopicConfig
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		if params, ok := call.Argument(0).Export().(map[string]any); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &topicConfig); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		k.createTopic(connection, topicConfig)
		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = connectionObject.Set("deleteTopic", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) > 0 {
			if topic, ok := call.Argument(0).Export().(string); !ok {
				common.Throw(runtime, ErrNotEnoughArguments)
			} else {
				k.deleteTopic(connection, topic)
			}
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = connectionObject.Set("listTopics", func(_ sobek.FunctionCall) sobek.Value {
		topics := k.listTopics(connection)
		return runtime.ToValue(topics)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	// partitionOffsets(topic[, sinceMillis]) reports the bounds of every partition of a topic, and the
	// offset a timestamp maps to. A reader without a consumer group can then start at a known offset,
	// which is the only way to read a past window: a group reader may only start at either end of the log.
	err = connectionObject.Set("partitionOffsets", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		var since *time.Time
		if len(call.Arguments) > 1 && !sobek.IsUndefined(call.Argument(1)) {
			at := time.UnixMilli(call.Argument(1).ToInteger())
			since = &at
		}

		offsets := k.partitionOffsets(connectionConfig, connection, call.Argument(0).String(), since)
		return runtime.ToValue(offsets)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = connectionObject.Set("close", func(_ sobek.FunctionCall) sobek.Value {
		if err := connection.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	return connectionObject
}

// getKafkaControllerConnection returns a kafka controller connection with a given node address.
// It will also try to use the auth and TLS settings to create a secure connection. The connection
// should be closed after use.
func (k *Kafka) getKafkaControllerConnection(connectionConfig *ConnectionConfig) *kafkago.Conn {
	dialer, wrappedError := GetDialer(connectionConfig.SASL, connectionConfig.TLS)
	if wrappedError != nil {
		logger.WithField("error", wrappedError).Error(wrappedError)
		if dialer == nil {
			common.Throw(k.vu.Runtime(), wrappedError)
			return nil
		}
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
		return nil
	}

	conn, err := dialer.DialContext(ctx, "tcp", connectionConfig.Address)
	if err != nil {
		wrappedError := NewXk6KafkaError(dialerError, "Failed to create dialer.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	controller, err := conn.Controller()
	if err != nil {
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	controllerConn, err := dialer.DialContext(
		ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	return controllerConn
}

// createTopic creates a topic with the given name, partitions, replication factor and compression.
// It will also try to use the auth and TLS settings to create a secure connection. If the topic
// already exists, it will do no-op.
func (k *Kafka) createTopic(conn *kafkago.Conn, topicConfig *kafkago.TopicConfig) {
	if topicConfig.NumPartitions <= 0 {
		topicConfig.NumPartitions = 1
	}

	if topicConfig.ReplicationFactor <= 0 {
		topicConfig.ReplicationFactor = 1
	}

	err := conn.CreateTopics(*topicConfig)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedCreateTopic, "Failed to create topic.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
	}
}

// deleteTopic deletes the given topic from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) deleteTopic(conn *kafkago.Conn, topic string) {
	err := conn.DeleteTopics([]string{topic}...)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedDeleteTopic, "Failed to delete topic.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
	}
}

// partitionOffsets reads the first and the last offset of every partition of the topic, plus the offset
// of the first record stored at or after `since` when one is given. Offsets live on the leader of each
// partition, so it dials the leaders rather than reusing the controller connection.
func (k *Kafka) partitionOffsets(
	connectionConfig *ConnectionConfig, conn *kafkago.Conn, topic string, since *time.Time,
) []map[string]any {
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedReadPartitions, "Failed to read partitions.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	dialer, wrappedError := GetDialer(connectionConfig.SASL, connectionConfig.TLS)
	if wrappedError != nil {
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
		return nil
	}

	offsets := make([]map[string]any, 0, len(partitions))
	for _, partition := range partitions {
		leader, err := dialer.DialLeader(ctx, "tcp", connectionConfig.Address, topic, partition.ID)
		if err != nil {
			wrappedError := NewXk6KafkaError(failedReadOffsets, "Failed to dial partition leader.", err)
			logger.WithField("error", wrappedError).Error(wrappedError)
			common.Throw(k.vu.Runtime(), wrappedError)
			return nil
		}

		first, last, err := leader.ReadOffsets()
		if err != nil {
			_ = leader.Close()
			wrappedError := NewXk6KafkaError(failedReadOffsets, "Failed to read offsets.", err)
			logger.WithField("error", wrappedError).Error(wrappedError)
			common.Throw(k.vu.Runtime(), wrappedError)
			return nil
		}

		offset := map[string]any{
			"partition":   partition.ID,
			"firstOffset": first,
			"lastOffset":  last,
		}

		if since != nil {
			at, err := leader.ReadOffset(*since)
			if err != nil {
				_ = leader.Close()
				wrappedError := NewXk6KafkaError(failedReadOffsets, "Failed to read offset at time.", err)
				logger.WithField("error", wrappedError).Error(wrappedError)
				common.Throw(k.vu.Runtime(), wrappedError)
				return nil
			}
			/*
			 * Kafka answers -1 when no record is stored at or after the timestamp. Reported as the end of
			 * the log, so that offsetAt always names a readable start and an empty window reads nothing:
			 * a caller passing -1 to a reader gets the whole partition instead.
			 */
			if at < first {
				at = last
			}
			offset["offsetAt"] = at
		}

		_ = leader.Close()
		offsets = append(offsets, offset)
	}

	return offsets
}

// listTopics lists the topics from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) listTopics(conn *kafkago.Conn) []string {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		wrappedError := NewXk6KafkaError(failedReadPartitions, "Failed to read partitions.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	// There should be a better way to return unique set of
	// topics instead of looping over them twice
	topicSet := map[string]struct{}{}

	for _, partition := range partitions {
		topicSet[partition.Topic] = struct{}{}
	}

	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	return topics
}
