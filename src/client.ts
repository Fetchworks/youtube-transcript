import type {
  ActorInput,
  ChannelOptions,
  FetchworksClientOptions,
  SearchOptions,
  TranscriptItem,
  TranscriptOptions,
} from "./types.js";

const ACTOR = "fetchworks~youtube-transcript-scraper";
/** Jobs expected to yield fewer videos than this run on the synchronous endpoint. */
const SYNC_LIMIT = 60;
const DATASET_PAGE_SIZE = 1000;

/** Error raised for any failed Apify API call or failed actor run. */
export class FetchworksError extends Error {
  /** HTTP status code, when the failure came from an HTTP response. */
  statusCode?: number;

  constructor(message: string, statusCode?: number) {
    super(message);
    this.name = "FetchworksError";
    this.statusCode = statusCode;
  }
}

interface RunRecord {
  id: string;
  status: string;
  defaultDatasetId: string;
}

/**
 * Thin client for the Fetchworks YouTube Transcript Scraper on Apify.
 * https://apify.com/fetchworks/youtube-transcript-scraper
 */
export class FetchworksClient {
  private readonly token: string;
  private readonly baseUrl: string;
  private readonly pollIntervalMs: number;
  private readonly maxWaitMs: number;

  constructor(options: FetchworksClientOptions) {
    if (!options?.apifyToken) {
      throw new FetchworksError(
        "apifyToken is required. Get one free at https://console.apify.com/settings/integrations",
      );
    }
    this.token = options.apifyToken;
    this.baseUrl = (options.baseUrl ?? "https://api.apify.com").replace(/\/+$/, "");
    this.pollIntervalMs = options.pollIntervalMs ?? 3000;
    this.maxWaitMs = options.maxWaitMs ?? 30 * 60 * 1000;
  }

  /** Transcript for a single video URL, Shorts/youtu.be/embed URL, or bare 11-character video ID. */
  async getTranscript(videoUrlOrId: string, options: TranscriptOptions = {}): Promise<TranscriptItem> {
    if (!videoUrlOrId) throw new FetchworksError("videoUrlOrId is required");
    const items = await this.run({ ...options, videoUrls: [videoUrlOrId] }, 1);
    const item = items[0];
    if (!item) throw new FetchworksError("actor returned no items for the video");
    return item;
  }

  /** Transcripts for a batch of video URLs or IDs. One item per video, in any order. */
  async getTranscripts(videoUrlsOrIds: string[], options: TranscriptOptions = {}): Promise<TranscriptItem[]> {
    if (!videoUrlsOrIds?.length) throw new FetchworksError("videoUrlsOrIds must be a non-empty array");
    return this.run({ ...options, videoUrls: videoUrlsOrIds }, videoUrlsOrIds.length);
  }

  /** Transcripts for a channel's uploads (newest first), bounded by maxVideosPerChannel (actor default 100). */
  async getChannelTranscripts(channelUrlOrHandle: string, options: ChannelOptions = {}): Promise<TranscriptItem[]> {
    if (!channelUrlOrHandle) throw new FetchworksError("channelUrlOrHandle is required");
    const { maxVideosPerChannel, ...rest } = options;
    const input: ActorInput = { ...rest, channelUrls: [channelUrlOrHandle] };
    if (maxVideosPerChannel !== undefined) input.maxVideosPerChannel = maxVideosPerChannel;
    return this.run(input, maxVideosPerChannel ?? 100);
  }

  /** Transcripts for a playlist URL or bare playlist ID. Size is unknown upfront, so this always runs async. */
  async getPlaylistTranscripts(playlistUrlOrId: string, options: TranscriptOptions = {}): Promise<TranscriptItem[]> {
    if (!playlistUrlOrId) throw new FetchworksError("playlistUrlOrId is required");
    return this.run({ ...options, playlistUrls: [playlistUrlOrId] }, Number.POSITIVE_INFINITY);
  }

  /** Transcripts for the top video results of a YouTube search query, bounded by maxSearchResults (actor default 50). */
  async search(query: string, options: SearchOptions = {}): Promise<TranscriptItem[]> {
    if (!query) throw new FetchworksError("query is required");
    const { maxSearchResults, ...rest } = options;
    const input: ActorInput = { ...rest, searchQueries: [query] };
    if (maxSearchResults !== undefined) input.maxSearchResults = maxSearchResults;
    return this.run(input, maxSearchResults ?? 50);
  }

  /** Run the actor with raw input. Small jobs run synchronously; larger ones start a run and poll. */
  async run(input: ActorInput, expectedVideos?: number): Promise<TranscriptItem[]> {
    const expected = expectedVideos ?? estimateVideos(input);
    if (expected < SYNC_LIMIT) return this.runSync(input);
    return this.runAsync(input);
  }

  private async runSync(input: ActorInput): Promise<TranscriptItem[]> {
    const res = await this.request(
      "POST",
      `/v2/acts/${ACTOR}/run-sync-get-dataset-items?token=${this.token}&clean=true`,
      input,
    );
    return res as TranscriptItem[];
  }

  private async runAsync(input: ActorInput): Promise<TranscriptItem[]> {
    const started = (await this.request("POST", `/v2/acts/${ACTOR}/runs?token=${this.token}`, input)) as {
      data: RunRecord;
    };
    const run = await this.waitForRun(started.data.id);
    if (run.status !== "SUCCEEDED") {
      throw new FetchworksError(`actor run ${run.id} finished with status ${run.status}`);
    }
    return this.collectDataset(run.defaultDatasetId);
  }

  private async waitForRun(runId: string): Promise<RunRecord> {
    const deadline = Date.now() + this.maxWaitMs;
    for (;;) {
      const res = (await this.request("GET", `/v2/actor-runs/${runId}?token=${this.token}`)) as { data: RunRecord };
      const { status } = res.data;
      if (status !== "READY" && status !== "RUNNING") return res.data;
      if (Date.now() >= deadline) {
        throw new FetchworksError(`timed out after ${this.maxWaitMs} ms waiting for run ${runId} (still ${status})`);
      }
      await sleep(this.pollIntervalMs);
    }
  }

  private async collectDataset(datasetId: string): Promise<TranscriptItem[]> {
    const items: TranscriptItem[] = [];
    for (let offset = 0; ; offset += DATASET_PAGE_SIZE) {
      const page = (await this.request(
        "GET",
        `/v2/datasets/${datasetId}/items?token=${this.token}&clean=true&format=json&offset=${offset}&limit=${DATASET_PAGE_SIZE}`,
      )) as TranscriptItem[];
      items.push(...page);
      if (page.length < DATASET_PAGE_SIZE) return items;
    }
  }

  private async request(method: "GET" | "POST", path: string, body?: unknown): Promise<unknown> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!res.ok) {
      let detail = "";
      try {
        const parsed = (await res.json()) as { error?: { message?: string } };
        detail = parsed?.error?.message ?? "";
      } catch {
        // non-JSON error body; the status line is enough
      }
      throw new FetchworksError(`Apify API ${method} ${res.status}${detail ? `: ${detail}` : ""}`, res.status);
    }
    return res.json();
  }
}

function estimateVideos(input: ActorInput): number {
  if (input.playlistUrls?.length) return Number.POSITIVE_INFINITY;
  let count = input.videoUrls?.length ?? 0;
  count += (input.channelUrls?.length ?? 0) * (input.maxVideosPerChannel ?? 100);
  count += (input.searchQueries?.length ?? 0) * (input.maxSearchResults ?? 50);
  return count;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
