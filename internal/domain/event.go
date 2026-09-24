package domain

import "time"

// Event is a raw fact from the outside world, delivered on the "raw-events" Kafka topic:
// a click, a login, a purchase. The stream processor consumes these and turns them into
// features (running averages, counts, rates).
//
// LEARNING NOTE — event time vs processing time
// ---------------------------------------------
// EventTime is when the thing actually happened (set by the producer). We deliberately do
// NOT trust "now" when computing windowed features, because events arrive late and out of
// order all the time (mobile client was offline, Kafka rebalanced, a consumer lagged).
// A window like "transactions in the last hour" must be defined over EVENT time, or the
// same input replayed tomorrow would produce different features — and reproducibility is
// the whole point of a feature store.
type Event struct {
	// EntityID identifies who/what this event is about, e.g. the user id.
	EntityID string `json:"entity_id"`

	// Entity is the entity type, e.g. "user_id". Together with EntityID it identifies
	// the aggregation bucket.
	Entity string `json:"entity"`

	// Type is the kind of event, e.g. "transaction", "login", "click". The stream
	// processor uses this to decide which features to update.
	Type string `json:"type"`

	// Amount is a generic numeric payload (transaction amount, cart value, ...).
	// A real system would carry an arbitrary payload; we keep one number to stay
	// focused on the aggregation mechanics, which are the interesting part.
	Amount float64 `json:"amount"`

	// EventTime is the logical time the event occurred. See the note above.
	EventTime time.Time `json:"event_time"`
}
