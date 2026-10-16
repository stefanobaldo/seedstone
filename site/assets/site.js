// The copy button. The page hides it where no script runs (a <noscript>
// style in the head), so the line stands alone there and nothing moves
// here when this file arrives.
for (const button of document.querySelectorAll("[data-copy]")) {
  const label = button.textContent;
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
      button.textContent = "Copied";
    } catch (e) {
      button.textContent = "Select and copy";
    }
    setTimeout(() => { button.textContent = label; }, 2000);
  });
}

// The install tabs. Without this file every panel shows under its own
// heading; with it the card gets `.js`, the headings become a tab row and
// one panel shows at a time. Arrow keys move between tabs and select.
const tabs = document.querySelector(".tabs");
if (tabs) {
  const list = [...tabs.querySelectorAll('[role="tab"]')];
  const select = (tab, focus) => {
    for (const t of list) {
      const on = t === tab;
      t.setAttribute("aria-selected", String(on));
      t.tabIndex = on ? 0 : -1;
      document.getElementById(t.getAttribute("aria-controls")).classList.toggle("on", on);
    }
    if (focus) tab.focus();
  };
  list.forEach((tab, i) => {
    tab.addEventListener("click", () => select(tab));
    tab.addEventListener("keydown", (e) => {
      const to = { ArrowRight: i + 1, ArrowLeft: i - 1, Home: 0, End: list.length - 1 }[e.key];
      if (to === undefined) return;
      e.preventDefault();
      select(list[(to + list.length) % list.length], true);
    });
  });
  select(list.find((t) => t.getAttribute("aria-selected") === "true") || list[0]);
  tabs.classList.add("js");
}

const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// The hero animation. The SVG's markup is its final frame; this driver
// clears it and replays it: the first run ticks along, the clients send
// their commands, the assertion fails at a tick and the trace prints;
// the second run, from the same seed, does the same; the check mark.
// Three rounds take about twenty seconds, so the picture carries a control
// while it moves: pause and play, then replay once it has rested. Under
// reduced motion the final frame stays as it is and the control stays
// hidden.
const anim = document.querySelector(".anim");
if (anim && !reduced) {
  const runs = anim.querySelectorAll(".run");
  const chips = anim.querySelectorAll(".chip");
  const clock = anim.querySelector(".clock");
  const verdict = anim.querySelector(".verdict");
  const control = document.querySelector(".anim-ctl");
  const TICK_MS = 70;
  let paused = false;
  let resume = null;

  // A pause lands at the next step: the step's own delay runs out, then
  // the driver waits for play.
  const wait = async (ms) => {
    await sleep(ms);
    if (paused) await new Promise((resolve) => { resume = resolve; });
  };

  const clear = (scope) => {
    for (const el of scope.querySelectorAll(".on")) el.classList.remove("on");
  };

  const run = async (lane) => {
    const cells = lane.querySelectorAll(".cell");
    for (const chip of chips) chip.classList.remove("on");
    for (let tick = 0; tick < cells.length; tick++) {
      cells[tick].classList.add("on");
      clock.textContent = `t = ${(tick / 10).toFixed(3)} s`;
      for (const chip of chips) {
        if (Number(chip.dataset.tick) === tick) chip.classList.add("on");
      }
      await wait(TICK_MS);
      if (cells[tick].classList.contains("hit")) break;
    }
    lane.querySelector(".trace").classList.add("on");
  };

  // Three times through, then the picture rests on its final frame, which
  // is what the markup already is after the third verdict.
  const play = async () => {
    control.textContent = "Pause";
    for (let round = 0; round < 3; round++) {
      clear(anim);
      clock.textContent = "t = 0.000 s";
      await wait(600);
      await run(runs[0]);
      await wait(1000);
      await run(runs[1]);
      await wait(400);
      verdict.classList.add("on");
      await wait(2000);
    }
    control.textContent = "Replay";
  };

  control.hidden = false;
  control.addEventListener("click", () => {
    if (control.textContent === "Replay") {
      play();
      return;
    }
    paused = !paused;
    control.textContent = paused ? "Play" : "Pause";
    if (!paused && resume) {
      const go = resume;
      resume = null;
      go();
    }
  });
  play();
}

// The three motions below the hero, each once, when scrolled into view.
// The markup is every visual's final state; each is taken back to its
// start here, as the page loads, and played by one observer the first
// time two fifths of it are on screen. Under reduced motion none of this
// runs and the final state stands.
if (!reduced && "IntersectionObserver" in window) {
  const plays = new Map();

  // The terminal: a prompt appears whole, what follows it is typed, a
  // reply prints at once.
  const typeTerm = async (term) => {
    for (const ln of term.querySelectorAll(".ln")) {
      const typed = ln.querySelector(".p")
        ? [...ln.childNodes].filter((n) => n.nodeType === 3)
        : [];
      const texts = typed.map((n) => n.data);
      for (const n of typed) n.data = "";
      ln.classList.add("on");
      if (typed.length) {
        ln.classList.add("typing");
        await sleep(350);
        for (const [i, n] of typed.entries()) {
          for (const ch of texts[i]) {
            n.data += ch;
            await sleep(35);
          }
        }
        ln.classList.remove("typing");
        await sleep(180);
      } else {
        await sleep(120);
      }
    }
    term.classList.remove("play");
  };
  const term = document.querySelector("#install .term");
  if (term) {
    term.classList.add("play");
    plays.set(term, () => typeTerm(term));
  }

  // A run panel: each strip lights its cells up to where its run ended,
  // lands its marker, and the verdict comes last.
  const playRuns = async (card) => {
    for (const run of card.querySelectorAll(".run")) {
      const cells = [...run.querySelectorAll("i")];
      const stop = cells.filter((c) => c.dataset.on).length;
      for (let tick = 0; tick < stop; tick++) {
        cells[tick].classList.add("on");
        await sleep(45);
      }
      run.querySelector(".tr").classList.add("on");
      await sleep(260);
    }
    for (const v of card.querySelectorAll(".verdict")) v.classList.add("on");
    card.classList.remove("play");
  };
  for (const card of document.querySelectorAll(".runs")) {
    card.classList.add("play");
    for (const cell of card.querySelectorAll(".cells i.on")) {
      cell.dataset.on = "1";
      cell.classList.remove("on");
    }
    plays.set(card, () => playRuns(card));
  }

  // The bars: `--w` is each bar's share; it goes to zero now and back to
  // its value in view, which the stylesheet transitions, row after row.
  for (const card of document.querySelectorAll(".compare")) {
    const bars = [...card.querySelectorAll(".bar")];
    bars.forEach((bar, i) => {
      bar.dataset.w = bar.style.getPropertyValue("--w");
      bar.style.setProperty("--w", "0%");
      bar.style.setProperty("--delay", `${i * 50}ms`);
    });
    plays.set(card, () => {
      for (const bar of bars) bar.style.setProperty("--w", bar.dataset.w);
    });
  }

  // The starts above are instant: the transitions are enabled only now,
  // after a reflow, so a bar already on screen does not shrink first.
  void document.body.offsetWidth;
  document.documentElement.classList.add("motion");

  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      const tall = entry.rootBounds && entry.intersectionRect.height >= entry.rootBounds.height * 0.5;
      if (entry.intersectionRatio >= 0.4 || tall) {
        observer.unobserve(entry.target);
        plays.get(entry.target)();
      }
    }
  }, { threshold: [0.4] });
  for (const target of plays.keys()) observer.observe(target);
}
