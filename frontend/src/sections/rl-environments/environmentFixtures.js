/**
 * PLACEHOLDER DATA — there is no `GET environments` endpoint on the harness yet.
 *
 * These rows mirror the agreed contract v0 shape so the list view can be built
 * and reviewed ahead of the API. When the endpoint lands, flip USE_FIXTURES to
 * false and pass real rows into <EnvironmentsListView environments={...} />;
 * nothing else in the view needs to change.
 */

export const USE_FIXTURES = false;

// Timestamps are epoch SECONDS as a float (that is what the harness emits).
// They are generated relative to module load so the "Updated" column keeps
// reading like a live list instead of drifting to "2 years ago".
const NOW_SECONDS = Date.now() / 1000;
const MINUTE = 60;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

const secondsAgo = (offset) => Number((NOW_SECONDS - offset).toFixed(3));

export const ENVIRONMENT_FIXTURES = [
  {
    session_id: "sess_9f1c2a4e",
    agent: "Drive-Thru Voice Agent",
    title: "Drive-thru ordering",
    one_liner:
      "Takes burger-and-fries orders at a drive-thru window, handles menu substitutions and upsells the combo.",
    created: secondsAgo(9 * DAY),
    updated: secondsAgo(12 * MINUTE),
    tools: 7,
    sub_goals: 5,
    scenarios: 14,
    runs: 14,
    runs_passed: 14,
    run_test_id: "rt_4c8d21",
    execution_id: "ex_77a3f0",
  },
  {
    session_id: "sess_3b7d5e10",
    agent: "Tier-1 Support Bot",
    title: "Billing support triage",
    one_liner:
      "Answers billing questions, issues refunds under $50 and escalates anything touching a disputed charge, a chargeback, a suspected fraudulent transaction or an account that has already been escalated twice in the same billing cycle to a human agent with the full conversation history attached.",
    created: secondsAgo(23 * DAY),
    updated: secondsAgo(3 * HOUR),
    tools: 12,
    sub_goals: 8,
    scenarios: 26,
    runs: 26,
    runs_passed: 21,
    run_test_id: "rt_9b0e57",
    execution_id: "ex_1d64bb",
  },
  {
    session_id: "sess_c40a8817",
    agent: "Clinic Booking Agent",
    title: "Appointment booking",
    one_liner:
      "Books, reschedules and cancels clinic appointments across three locations while respecting practitioner availability.",
    created: secondsAgo(4 * DAY),
    updated: secondsAgo(26 * HOUR),
    tools: 5,
    sub_goals: 4,
    scenarios: 9,
    runs: 9,
    runs_passed: 7,
    run_test_id: "rt_2f19ac",
    execution_id: "ex_58c902",
  },
  {
    session_id: "sess_7e22d9b3",
    agent: "Outbound Sales SDR",
    title: "Cold outreach qualifier",
    one_liner:
      "Qualifies inbound leads against BANT and books a demo slot when the lead clears the bar.",
    created: secondsAgo(2 * DAY),
    updated: secondsAgo(45 * MINUTE),
    tools: 6,
    sub_goals: 6,
    scenarios: 11,
    // Authored but never executed — the runs chip should read neutral.
    runs: 0,
    runs_passed: 0,
    run_test_id: null,
    execution_id: null,
  },
  {
    session_id: "sess_18ff6a05",
    agent: "Travel Rebooking Agent",
    title: "Disrupted flight rebooking",
    one_liner:
      "Rebooks passengers after a cancellation, honouring fare class, checked bags and connecting segments.",
    created: secondsAgo(41 * DAY),
    updated: secondsAgo(6 * DAY),
    tools: 15,
    sub_goals: 11,
    scenarios: 38,
    runs: 34,
    runs_passed: 29,
    run_test_id: "rt_6ad330",
    execution_id: "ex_0b91e4",
  },
  {
    session_id: "sess_bb904c76",
    agent: "Insurance Claims Intake",
    title: "First notice of loss",
    one_liner:
      "Collects a first notice of loss for motor claims and validates the policy is active on the incident date.",
    created: secondsAgo(15 * DAY),
    updated: secondsAgo(2 * DAY),
    tools: 9,
    sub_goals: 7,
    scenarios: 18,
    runs: 18,
    runs_passed: 18,
    run_test_id: "rt_a71c48",
    execution_id: "ex_3e5d17",
  },
  {
    session_id: "sess_d5e13f92",
    agent: "Restaurant Reservation Host",
    title: "Table reservations",
    one_liner:
      "Handles table reservations, party-size changes and waitlist callbacks for a two-floor restaurant.",
    created: secondsAgo(6 * DAY),
    updated: secondsAgo(5 * HOUR),
    tools: 4,
    sub_goals: 3,
    scenarios: 7,
    runs: 7,
    runs_passed: 5,
    run_test_id: "rt_ce2210",
    execution_id: null,
  },
  {
    session_id: "sess_20a6c7de",
    agent: "IT Helpdesk Assistant",
    title: "Password and access requests",
    one_liner:
      "Resets passwords, unlocks accounts and grants scoped repo access after checking the requester's manager.",
    created: secondsAgo(31 * DAY),
    updated: secondsAgo(9 * DAY),
    tools: 10,
    sub_goals: 6,
    scenarios: 21,
    runs: 21,
    runs_passed: 21,
    run_test_id: "rt_88b4f1",
    execution_id: "ex_c206aa",
  },
  {
    session_id: "sess_f6c80b41",
    // No agent name yet — the Name column falls back to the title.
    agent: null,
    title: "Grocery substitution picker",
    one_liner:
      "Picks acceptable substitutes for out-of-stock grocery items and confirms the swap with the shopper.",
    created: secondsAgo(1 * DAY),
    updated: secondsAgo(35 * MINUTE),
    tools: 3,
    sub_goals: 2,
    scenarios: 5,
    runs: 0,
    runs_passed: 0,
    run_test_id: null,
    execution_id: null,
  },
  {
    // Neither agent nor title — the Name column falls back to the session id.
    session_id: "sess_e93172ca",
    agent: null,
    title: null,
    one_liner:
      "Untitled draft environment captured from a live call transcript, contract not yet confirmed.",
    created: secondsAgo(4 * HOUR),
    updated: secondsAgo(4 * HOUR),
    tools: 2,
    sub_goals: 1,
    scenarios: 0,
    runs: 0,
    runs_passed: 0,
    run_test_id: null,
    execution_id: null,
  },
];

/**
 * Hook-shaped accessor so the swap to a real React Query hook is a one-liner at
 * the call site. Deliberately synchronous — there is no request to make.
 */
export const useEnvironmentFixtures = () => ({
  // The flag is what decides, so turning the placeholders off shows the real empty state
  // rather than a list nobody can click.
  environments: USE_FIXTURES ? ENVIRONMENT_FIXTURES : [],
  isLoading: false,
  error: null,
});
