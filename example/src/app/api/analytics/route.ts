import { NextResponse } from "next/server";
import {
  tinybird,
  type TopPagesParams,
  type TopEventsParams,
} from "@tinybird/client";

export async function GET() {
  // Check if token is configured
  if (!process.env.TINYBIRD_TOKEN) {
    return NextResponse.json({
      error: "TINYBIRD_TOKEN not configured. Add it to your .env.local file.",
      topPages: [],
      topEvents: [],
    });
  }

  try {
    // Get date range for last 30 days
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 30);

    // Type-safe params
    const pagesParams: TopPagesParams = {
      start_date: startDate.toISOString().replace("T", " ").slice(0, 19),
      end_date: endDate.toISOString().replace("T", " ").slice(0, 19),
      limit: 10,
    };

    const eventsParams: TopEventsParams = {
      start_date: startDate.toISOString().replace("T", " ").slice(0, 19),
      end_date: endDate.toISOString().replace("T", " ").slice(0, 19),
      limit: 10,
    };

    // Fetch in parallel
    const [topPagesResult, topEventsResult] = await Promise.all([
      tinybird.topPages.query(pagesParams),
      tinybird.topEvents.query(eventsParams),
    ]);

    return NextResponse.json({
      topPages: topPagesResult.data,
      topEvents: topEventsResult.data,
    });
  } catch (error) {
    console.error("Analytics error:", error);
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Unknown error",
        topPages: [],
        topEvents: [],
      },
      { status: 500 }
    );
  }
}
