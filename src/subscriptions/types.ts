// Public types for the subscription layer (M4).

export type SubscriptionHandler<T = unknown> = (status: T) => void;

/** Returned by `manager.scripthash.subscribe(...)` etc. — call to remove the handler. */
export type Unsubscribe = () => Promise<void>;
