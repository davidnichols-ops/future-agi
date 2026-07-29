// Local, prototype-only invite store for the OSS self-host demo.
//
// The real RBAC invite/email endpoints 401 under the fake proto session, so on
// a local OSS instance we manage invites entirely client-side: each invite gets
// a shareable link, and whether an email is "sent" depends on a locally
// configured SMTP server (a mail catcher or a real provider). Everything lives
// in localStorage so it survives in-session navigation and is wiped by the
// demo reset on a fresh tab. On a real OSS backend the live flow is used instead.

import { paths } from "src/routes/paths";
import { orgLevelToString, LEVELS } from "../constant";

export const isProtoSession = () =>
  import.meta.env.VITE_PROTOTYPE_AUTH_BYPASS === "true" &&
  localStorage.getItem("oss_proto_session") === "1";

const INVITES_KEY = "oss_invites";
const SMTP_KEY = "oss_smtp";

export const INVITES_EVENT = "oss-invites-changed";
export const SMTP_EVENT = "oss-smtp-changed";

const read = (key) => {
  try {
    return JSON.parse(localStorage.getItem(key));
  } catch {
    return null;
  }
};

// ── invites ────────────────────────────────────────────────────────────────
export const getInvites = () => read(INVITES_KEY) || [];

const writeInvites = (list) => {
  localStorage.setItem(INVITES_KEY, JSON.stringify(list));
  window.dispatchEvent(new Event(INVITES_EVENT));
};

export const removeInvite = (id) => {
  writeInvites(getInvites().filter((i) => i.id !== id));
};

const uuid = () =>
  (typeof crypto !== "undefined" && crypto.randomUUID
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`
  ).replace(/-/g, "");

export const makeInviteLink = (token, email) =>
  `${window.location.origin}${paths.auth.jwt.register}?invite=${token}` +
  (email ? `&email=${encodeURIComponent(email)}` : "");

// Create + persist invites for a batch of emails at a single org level.
// Each invite records how it was "delivered" (see DELIVERY).
export const createInvites = (emails, orgLevel) => {
  const delivery = getDelivery();
  const nowIso = new Date().toISOString();
  const created = emails.map((email) => {
    const token = uuid();
    return {
      id: token,
      email,
      name: email.split("@")[0],
      org_role: orgLevelToString[orgLevel] || "Member",
      org_level: orgLevel,
      ws_role: orgLevel >= LEVELS.ADMIN ? "Workspace Admin" : "Workspace Member",
      status: "Pending",
      created_at: nowIso,
      invite_link: makeInviteLink(token, email),
      delivery,
    };
  });
  writeInvites([...created, ...getInvites()]);
  return created;
};

// Grid-row shape expected by the Members table (mirrors a Pending member).
export const inviteToRow = (inv) => ({
  id: inv.id,
  name: inv.name,
  email: inv.email,
  org_role: inv.org_role,
  org_level: inv.org_level,
  ws_role: inv.ws_role,
  status: "Pending",
  created_at: inv.created_at,
  workspaces: [],
  type: "invite",
  invite_link: inv.invite_link,
  delivery: inv.delivery,
});

// ── SMTP / email delivery ────────────────────────────────────────────────────
// Three honest states for a self-host instance:
//   NONE     — no email set up; admin copies each link and shares it manually.
//   CATCHER  — a LOCAL mail catcher (Mailpit/MailHog) intercepts the mail on the
//              admin's own machine. The invitee receives NOTHING; the admin opens
//              the catcher inbox, copies the link, and shares it themselves.
//   PROVIDER — a real SMTP provider actually delivers the email to the invitee.
//              (On a localhost instance the link still only opens on this machine.)
export const DELIVERY = { NONE: "none", CATCHER: "catcher", PROVIDER: "provider" };

export const getSmtp = () => read(SMTP_KEY);
export const isSmtpConfigured = () => !!getSmtp();

export const getDelivery = () => {
  const s = getSmtp();
  if (!s) return DELIVERY.NONE;
  return s.kind === "provider" ? DELIVERY.PROVIDER : DELIVERY.CATCHER;
};

// Default inbox where a local mail catcher shows intercepted mail.
export const getCatcherUrl = () => getSmtp()?.catcherUrl || "http://localhost:8025";

export const saveSmtp = (cfg) => {
  localStorage.setItem(SMTP_KEY, JSON.stringify(cfg));
  window.dispatchEvent(new Event(SMTP_EVENT));
};

export const clearSmtp = () => {
  localStorage.removeItem(SMTP_KEY);
  window.dispatchEvent(new Event(SMTP_EVENT));
};

// True when the app is served from a local host (invite links only reachable
// on this machine) — drives the "expose a real domain" caveat.
export const isLocalInstance = () =>
  /^(localhost$|127\.|0\.0\.0\.0$|.*\.local(host)?$|.*\.futureagi\.com$)/.test(
    window.location.hostname,
  );
