import { describe, it, expect } from "vitest";

// The endpoint returns bare strings by default and {key, type} objects under
// `include_types`. Typing the response as object-only silently broke response
// validation for every legacy caller, so pin both shapes.
import { validateContractedResponse } from "src/api/contracts/openapi-contract.js";

const call = (result) =>
  validateContractedResponse({
    status: 200,
    data: { status: true, result },
    config: {
      method: "get",
      url: "/tracer/observation-span/get_eval_attributes_list/?filters=%7B%7D",
    },
  });

describe("attribute list response contract accepts both shapes", () => {
  it("legacy flat strings (the 11 existing callers)", () => {
    const r = call(["customer_tier", "retry_count"]);
    expect(r.ok, r.ok ? "" : r.error.message).toBe(true);
  });
  it("typed objects (include_types=true)", () => {
    const r = call([{ key: "retry_count", type: "number" }]);
    expect(r.ok, r.ok ? "" : r.error.message).toBe(true);
  });
});
