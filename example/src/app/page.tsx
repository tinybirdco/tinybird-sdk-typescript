"use client";

import { useState, useEffect, useCallback } from "react";
import type {
  TopPagesOutput,
  TopEventsOutput,
  PageViewsRow,
  EventsRow,
} from "@tinybird/client";

export default function Home() {
  const [topPages, setTopPages] = useState<TopPagesOutput[]>([]);
  const [topEvents, setTopEvents] = useState<TopEventsOutput[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const fetchAnalytics = useCallback(async () => {
    try {
      const res = await fetch("/api/analytics");
      const data = await res.json();
      if (data.error) {
        setError(data.error);
      } else {
        setTopPages(data.topPages);
        setTopEvents(data.topEvents);
        setError(null);
        setLastUpdated(new Date());
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
    }
  }, []);

  // Poll every second
  useEffect(() => {
    fetchAnalytics();
    const interval = setInterval(fetchAnalytics, 1000);
    return () => clearInterval(interval);
  }, [fetchAnalytics]);

  const trackPageView = async () => {
    const pageView: PageViewsRow = {
      timestamp: new Date(),
      session_id: crypto.randomUUID(),
      user_id: null,
      pathname: window.location.pathname,
      referrer: document.referrer || null,
      user_agent: navigator.userAgent,
      country: null,
      device_type: "desktop",
    };

    try {
      await fetch("/api/track", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "pageview", data: pageView }),
      });
    } catch (e) {
      console.error("Failed to track:", e);
    }
  };

  const trackEvent = async () => {
    const event: EventsRow = {
      timestamp: new Date(),
      session_id: crypto.randomUUID(),
      user_id: null,
      event_name: "button_click",
      properties: JSON.stringify({
        button: "track_event",
        page: window.location.pathname,
      }),
    };

    try {
      await fetch("/api/track", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "event", data: event }),
      });
    } catch (e) {
      console.error("Failed to track:", e);
    }
  };

  return (
    <div className="min-h-screen bg-zinc-50 dark:bg-zinc-900 p-8">
      <main className="max-w-4xl mx-auto">
        <h1 className="text-3xl font-bold text-zinc-900 dark:text-white mb-2">
          @tinybird/sdk Demo
        </h1>
        <p className="text-zinc-600 dark:text-zinc-400 mb-2">
          Real-time analytics powered by the Tinybird TypeScript SDK.
        </p>
        {lastUpdated && (
          <p className="text-zinc-400 text-sm mb-8">
            Last updated: {lastUpdated.toLocaleTimeString()}
          </p>
        )}

        {/* Actions */}
        <div className="flex flex-wrap gap-4 mb-8">
          <button
            onClick={trackPageView}
            className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
          >
            Track Page View
          </button>
          <button
            onClick={trackEvent}
            className="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700"
          >
            Track Event
          </button>
        </div>

        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-8">
            <strong>Error:</strong> {error}
          </div>
        )}

        {/* Results */}
        <div className="grid md:grid-cols-2 gap-8">
          {/* Top Pages */}
          <div className="bg-white dark:bg-zinc-800 rounded-xl p-6 shadow-sm">
            <h2 className="text-xl font-semibold text-zinc-900 dark:text-white mb-4">
              Top Pages
            </h2>
            {topPages.length === 0 ? (
              <p className="text-zinc-500">
                No data yet. Click &quot;Track Page View&quot; to add some.
              </p>
            ) : (
              <table className="w-full">
                <thead>
                  <tr className="text-left text-zinc-500 text-sm">
                    <th className="pb-2">Path</th>
                    <th className="pb-2 text-right">Views</th>
                    <th className="pb-2 text-right">Sessions</th>
                  </tr>
                </thead>
                <tbody>
                  {topPages.map((page, i) => (
                    <tr
                      key={i}
                      className="border-t border-zinc-100 dark:border-zinc-700"
                    >
                      <td className="py-2 text-zinc-900 dark:text-white font-mono text-sm">
                        {page.pathname}
                      </td>
                      <td className="py-2 text-right text-zinc-600 dark:text-zinc-400">
                        {page.views.toLocaleString()}
                      </td>
                      <td className="py-2 text-right text-zinc-600 dark:text-zinc-400">
                        {page.unique_sessions.toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Top Events */}
          <div className="bg-white dark:bg-zinc-800 rounded-xl p-6 shadow-sm">
            <h2 className="text-xl font-semibold text-zinc-900 dark:text-white mb-4">
              Top Events
            </h2>
            {topEvents.length === 0 ? (
              <p className="text-zinc-500">
                No data yet. Click &quot;Track Event&quot; to add some.
              </p>
            ) : (
              <table className="w-full">
                <thead>
                  <tr className="text-left text-zinc-500 text-sm">
                    <th className="pb-2">Event</th>
                    <th className="pb-2 text-right">Count</th>
                    <th className="pb-2 text-right">Sessions</th>
                  </tr>
                </thead>
                <tbody>
                  {topEvents.map((event, i) => (
                    <tr
                      key={i}
                      className="border-t border-zinc-100 dark:border-zinc-700"
                    >
                      <td className="py-2 text-zinc-900 dark:text-white font-mono text-sm">
                        {event.event_name}
                      </td>
                      <td className="py-2 text-right text-zinc-600 dark:text-zinc-400">
                        {event.event_count.toLocaleString()}
                      </td>
                      <td className="py-2 text-right text-zinc-600 dark:text-zinc-400">
                        {event.unique_sessions.toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* Code Example */}
        <div className="mt-8 bg-zinc-900 rounded-xl p-6 overflow-x-auto">
          <h2 className="text-lg font-semibold text-white mb-4">
            Type-Safe Usage Example
          </h2>
          <pre className="text-sm text-green-400 font-mono">
            {`// Define your datasource schema
const pageViews = defineDatasource("page_views", {
  schema: {
    timestamp: t.dateTime(),
    pathname: t.string(),
    user_id: t.string().nullable(),
  },
  engine: engine.mergeTree({
    sortingKey: ["pathname", "timestamp"],
  }),
});

// Infer types automatically
type PageViewRow = InferRow<typeof pageViews>;
// { timestamp: Date; pathname: string; user_id: string | null }

// Full autocomplete and type checking!
const event: PageViewRow = {
  timestamp: new Date(),
  pathname: "/home",
  user_id: null,
};`}
          </pre>
        </div>
      </main>
    </div>
  );
}
