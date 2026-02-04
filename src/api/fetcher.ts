export const TINYBIRD_FROM_PARAM = "ts-sdk";

export type TinybirdFetch = (url: string, init?: RequestInit) => Promise<Response>;

export function withTinybirdFromParam(url: string): string {
  const parsedUrl = new URL(url);
  parsedUrl.searchParams.set("from", TINYBIRD_FROM_PARAM);
  return parsedUrl.toString();
}

export function createTinybirdFetcher(fetchFn: typeof fetch): TinybirdFetch {
  return (url, init) => fetchFn(withTinybirdFromParam(url), init);
}

export function tinybirdFetch(url: string, init?: RequestInit): Promise<Response> {
  return fetch(withTinybirdFromParam(url), init);
}
