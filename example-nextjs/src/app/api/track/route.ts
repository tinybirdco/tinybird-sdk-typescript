import { NextRequest, NextResponse } from "next/server";
import { tinybird, type PageViewRow, type EventRow } from "@/tinybird/client";

interface TrackRequest {
  type: "pageview" | "event";
  data: PageViewRow | EventRow;
}

export async function POST(request: NextRequest) {
  // Check if token is configured
  if (!process.env.TINYBIRD_TOKEN) {
    return NextResponse.json(
      { error: "TINYBIRD_TOKEN not configured" },
      { status: 500 }
    );
  }

  try {
    const body = (await request.json()) as TrackRequest;

    if (body.type === "pageview") {
      // Type-safe ingestion
      await tinybird.ingest.pageView(body.data as PageViewRow);
      return NextResponse.json({ success: true, type: "pageview" });
    } else if (body.type === "event") {
      // Type-safe ingestion
      await tinybird.ingest.event(body.data as EventRow);
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
