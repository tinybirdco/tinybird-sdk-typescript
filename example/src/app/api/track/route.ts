import { NextRequest, NextResponse } from "next/server";
import { tinybird, type PageViewsRow, type EventsRow } from "@tinybird/client";

interface TrackRequest {
  type: "pageview" | "event";
  data: PageViewsRow | EventsRow;
}

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as TrackRequest;

    if (body.type === "pageview") {
      await tinybird.pageViews.ingest(body.data as PageViewsRow);
      return NextResponse.json({ success: true, type: "pageview" });
    } else if (body.type === "event") {
      await tinybird.events.ingest(body.data as EventsRow);
      return NextResponse.json({ success: true, type: "event" });
    } else {
      return NextResponse.json(
        { error: "Invalid event type" },
        { status: 400 }
      );
    }
  } catch (error) {
    console.error("Track error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
