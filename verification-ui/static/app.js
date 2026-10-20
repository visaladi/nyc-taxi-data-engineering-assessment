const LABELS = { pass: "Pass", warn: "Warn", fail: "Fail", info: "Info" };
const REFRESH_MS = 30000;

const $ = (id) => document.getElementById(id);
let lastData = null;
let timer = null;

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function pill(status, text) {
  return el("span", `pill pill-${status}`, text ?? LABELS[status]);
}

function countByStatus(checks) {
  const counts = { pass: 0, warn: 0, fail: 0, info: 0 };
  checks.forEach((c) => { counts[c.status] = (counts[c.status] || 0) + 1; });
  return counts;
}

function render(data) {
  const all = data.groups.flatMap((g) => g.checks);
  const counts = countByStatus(all);
  for (const s of ["pass", "warn", "fail", "info"]) $(`n-${s}`).textContent = counts[s];
  $("last-run").textContent = new Date(data.generated_at).toLocaleTimeString();
  $("duration").textContent = `${all.length} checks in ${data.duration_s} s`;

  const links = $("links");
  links.replaceChildren(...data.links.map((l) => {
    const a = el("a");
    a.href = l.url;
    a.target = "_blank";
    a.rel = "noopener";
    a.append(document.createTextNode(l.label), el("span", "", l.note));
    return a;
  }));

  const onlyProblems = $("only-problems").checked;
  const groups = data.groups.map((group) => {
    const section = el("section", "group");
    const head = el("div", "group-head");
    head.append(el("h2", "", group.title));
    const gc = countByStatus(group.checks);
    const badges = el("div", "group-counts");
    for (const s of ["fail", "warn", "pass", "info"]) {
      if (gc[s]) badges.append(pill(s, `${gc[s]} ${LABELS[s].toLowerCase()}`));
    }
    head.append(badges);
    section.append(head);

    for (const c of group.checks) {
      const row = el("div", "check");
      if (onlyProblems && (c.status === "pass" || c.status === "info")) row.classList.add("hidden");
      const statusCell = el("div");
      statusCell.append(pill(c.status));
      row.append(
        statusCell,
        el("div", "check-name", c.name),
        el("div", "check-value", c.value),
        el("div", "check-detail", c.detail),
        el("div", "check-ms", `${c.ms} ms`),
      );
      section.append(row);
    }
    return section;
  });
  $("groups").replaceChildren(...groups);

  const line = $("status-line");
  line.classList.remove("error");
  line.textContent = counts.fail
    ? `${counts.fail} check(s) failed — see the red rows below.`
    : counts.warn
      ? `All critical checks pass; ${counts.warn} warning(s) to review.`
      : "Everything verified.";
}

async function runChecks() {
  const button = $("run");
  if (button.disabled) return; // a run is already in progress
  button.disabled = true;
  button.textContent = "Running…";
  if (!lastData) $("status-line").textContent = "Running checks…";
  try {
    const resp = await fetch("/api/checks", { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    lastData = await resp.json();
    render(lastData);
  } catch (err) {
    const line = $("status-line");
    line.classList.add("error");
    line.textContent = `Could not run checks: ${err.message}. Is verification-ui/server.py running?`;
  } finally {
    button.disabled = false;
    button.textContent = "Run checks";
  }
}

$("run").addEventListener("click", runChecks);
$("only-problems").addEventListener("change", () => lastData && render(lastData));
$("auto-refresh").addEventListener("change", (e) => {
  clearInterval(timer);
  if (e.target.checked) timer = setInterval(runChecks, REFRESH_MS);
});

runChecks();
