// Projects the TLA+ variables onto the words a SPARC reader uses.
// The command calls these functions at most once per model state or step.

const roleState = (s) => {
  if (s === "running") return "Running";
  if (s === "stopped") return "Stopped";
  if (s === "crashed") return "Crashed";
  return "Not started";
};

const messageState = (v) => {
  if (v.reserving) return "Writing";
  if (v.reading) return "Reading";
  if (v.tail === v.head) return "Empty";
  return "Full";
};

export const state = (v) => ({
  Producer: roleState(v.producer),
  Consumer: roleState(v.consumer),
  Messages: messageState(v),
});

const STEPS = {
  ProducerStarts: "the producer starts",
  ProducerReserves: "the producer reserves a slot",
  ProducerCommits: "the producer publishes the message",
  ProducerCrashes: "the producer is killed",
  ProducerStops: "the producer stops",
  ConsumerStarts: "the consumer starts",
  ConsumerPeeks: "the consumer copies a message",
  ConsumerAdvances: "the consumer releases the slot",
  ConsumerCrashes: "the consumer is killed mid-read",
  ConsumerStops: "the consumer stops",
};

export const step = (action) => STEPS[action] ?? action;

export const title = "SPARC endpoint and message lifecycle";
