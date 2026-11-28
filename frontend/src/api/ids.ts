/**
 * A fresh id for an `external_order_id` or a status `event_id`. Make one per
 * user action and send the same one again on a retry: that is what makes the
 * server treat the retry as the same request.
 */
export function newId(): string {
  return crypto.randomUUID();
}
