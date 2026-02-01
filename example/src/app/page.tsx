"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import type { TopEventsOutput, EventsRow } from "@tinybird/client";

// Color mapping for different events
const eventColors: Record<string, string> = {
  signup: "bg-green-500",
  purchase: "bg-blue-500",
};

export default function Home() {
  const [topEvents, setTopEvents] = useState<TopEventsOutput[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Click streak state
  const [signupStreak, setSignupStreak] = useState(0);
  const [purchaseStreak, setPurchaseStreak] = useState(0);
  const signupTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const purchaseTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchAnalytics = useCallback(async () => {
    setIsRefreshing(true);
    try {
      const res = await fetch("/api/analytics");
      const data = await res.json();
      if (data.error) {
        setError(data.error);
      } else {
        setTopEvents(data.topEvents);
        setError(null);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setIsRefreshing(false);
    }
  }, []);

  // Initial fetch
  useEffect(() => {
    fetchAnalytics();
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

    // Update streak
    if (eventName === "signup") {
      if (signupTimeoutRef.current) {
        clearTimeout(signupTimeoutRef.current);
      }
      setSignupStreak((prev) => prev + 1);
      signupTimeoutRef.current = setTimeout(() => {
        setSignupStreak(0);
      }, 1500);
    } else if (eventName === "purchase") {
      if (purchaseTimeoutRef.current) {
        clearTimeout(purchaseTimeoutRef.current);
      }
      setPurchaseStreak((prev) => prev + 1);
      purchaseTimeoutRef.current = setTimeout(() => {
        setPurchaseStreak(0);
      }, 1500);
    }

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
          @tinybirdco/sdk Demo
        </h1>
        <p className="text-zinc-600 dark:text-zinc-400 mb-8">
          Real-time analytics powered by the Tinybird TypeScript SDK.
        </p>

        {/* Actions */}
        <div className="flex flex-wrap gap-4 mb-8">
          <button
            onClick={() => trackEvent("signup")}
            className="relative px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 font-medium transition-transform active:scale-95"
          >
            Track Signup
            {signupStreak > 0 && (
              <span className="absolute -top-2 -right-2 bg-green-800 text-white text-xs font-bold px-2 py-1 rounded-full animate-bounce">
                +{signupStreak}
              </span>
            )}
          </button>
          <button
            onClick={() => trackEvent("purchase")}
            className="relative px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium transition-transform active:scale-95"
          >
            Track Purchase
            {purchaseStreak > 0 && (
              <span className="absolute -top-2 -right-2 bg-blue-800 text-white text-xs font-bold px-2 py-1 rounded-full animate-bounce">
                +{purchaseStreak}
              </span>
            )}
          </button>
        </div>

        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-8">
            <strong>Error:</strong> {error}
          </div>
        )}

        {/* Top Events Chart */}
        <div className="bg-white dark:bg-zinc-800 rounded-xl p-6 shadow-sm">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-zinc-900 dark:text-white">
              Events
            </h2>
            <button
              onClick={fetchAnalytics}
              disabled={isRefreshing}
              className="p-2 text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-700 rounded-lg transition-colors disabled:opacity-50"
              title="Refresh"
            >
              <svg
                className={`w-5 h-5 ${isRefreshing ? "animate-spin" : ""}`}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                />
              </svg>
            </button>
          </div>
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
