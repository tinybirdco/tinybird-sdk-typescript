"use client";

import { useState, useEffect, useCallback } from "react";
import type { TopEventsOutput, EventsRow } from "@tinybird/client";

// Color mapping for different events
const eventColors: Record<string, string> = {
  signup: "bg-green-500",
  purchase: "bg-blue-500",
};

export default function Home() {
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

  const trackEvent = async (eventName: string) => {
    const event: EventsRow = {
      timestamp: new Date(),
      session_id: crypto.randomUUID(),
      user_id: null,
      event_name: eventName,
      properties: JSON.stringify({
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
      <main className="max-w-2xl mx-auto">
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
            onClick={() => trackEvent("signup")}
            className="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 font-medium"
          >
            Track Signup
          </button>
          <button
            onClick={() => trackEvent("purchase")}
            className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium"
          >
            Track Purchase
          </button>
        </div>

        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-8">
            <strong>Error:</strong> {error}
          </div>
        )}

        {/* Top Events Chart */}
        <div className="bg-white dark:bg-zinc-800 rounded-xl p-6 shadow-sm">
          <h2 className="text-xl font-semibold text-zinc-900 dark:text-white mb-4">
            Events
          </h2>
          {topEvents.length === 0 ? (
            <p className="text-zinc-500">
              No data yet. Click a button above to track an event.
            </p>
          ) : (
            <div className="space-y-4">
              {topEvents.map((event, i) => {
                const maxCount = Math.max(
                  ...topEvents.map((e) => e.event_count)
                );
                const percentage = (event.event_count / maxCount) * 100;
                const barColor =
                  eventColors[event.event_name] || "bg-purple-500";
                return (
                  <div key={i}>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="text-zinc-900 dark:text-white font-medium">
                        {event.event_name}
                      </span>
                      <span className="text-zinc-500">
                        {event.event_count.toLocaleString()}
                      </span>
                    </div>
                    <div className="h-3 bg-zinc-100 dark:bg-zinc-700 rounded-full overflow-hidden">
                      <div
                        className={`h-full ${barColor} rounded-full transition-all duration-300`}
                        style={{ width: `${percentage}%` }}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
