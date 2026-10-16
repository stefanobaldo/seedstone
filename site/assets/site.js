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
      const panel = document.getElementById(t.getAttribute("aria-controls"));
      panel.setAttribute("role", "tabpanel");   // a tab panel only where there are tabs
      panel.classList.toggle("on", on);
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

// The motions below the hero, each time one is scrolled into view: the
// two terminals type, the two run panels run, the comparison bars grow.
// The markup is every visual's final state; each is taken back to its
// start here, as the page loads, played by one observer when two fifths
// of it are on screen, and taken back to its start again once it has
// left the screen whole, so it plays again on the next entry. Under
// reduced motion none of this runs and the final state stands.
if (!reduced && "IntersectionObserver" in window) {
  const motions = new Map();   // target → { play, reset, armed, running, resetWhenDone }

  // The terminal: a prompt appears whole, what follows it is typed, a
  // reply prints at once. What follows a prompt is split into two spans,
  // the typed part and the rest, and typing moves characters from one to
  // the other; the text is in the page whole throughout, the rest in
  // transparent ink, so the line keeps its height and a screen reader has
  // the whole transcript at any moment.
  const resetTerm = (term) => {
    term.classList.add("play");
    for (const ln of term.querySelectorAll(".ln")) {
      ln.classList.remove("on", "typing");
      if (ln.querySelector(".p") && !ln.querySelector(".todo")) {
        const texts = [...ln.childNodes].filter((n) => n.nodeType === 3);
        const typed = document.createElement("span");
        const todo = document.createElement("span");
        typed.className = "typed";
        todo.className = "todo";
        // The space after the prompt is the prompt's, not typed: the
        // cursor waits where the command begins.
        const text = texts.map((n) => n.data).join("");
        const lead = text.match(/^\s*/)[0];
        todo.textContent = text.slice(lead.length);
        texts[0].replaceWith(document.createTextNode(lead), typed, todo);
        for (const n of texts.slice(1)) n.remove();
      }
      const todo = ln.querySelector(".todo");
      if (todo) {
        const typed = ln.querySelector(".typed");
        todo.textContent = typed.textContent + todo.textContent;
        typed.textContent = "";
      }
    }
  };
  const typeTerm = async (term) => {
    for (const ln of term.querySelectorAll(".ln")) {
      const todo = ln.querySelector(".todo");
      ln.classList.add("on");
      if (todo) {
        const typed = ln.querySelector(".typed");
        ln.classList.add("typing");
        await sleep(350);
        while (todo.textContent) {
          typed.textContent += todo.textContent[0];
          todo.textContent = todo.textContent.slice(1);
          await sleep(35);
        }
        ln.classList.remove("typing");
        await sleep(180);
      } else {
        await sleep(120);
      }
    }
    term.classList.remove("play");
  };
  for (const term of document.querySelectorAll(".term")) {
    motions.set(term, { play: () => typeTerm(term), reset: () => resetTerm(term) });
  }

  // A run panel: each strip lights its cells up to where its run ended,
  // lands its marker, and the verdict comes last.
  const resetRuns = (card) => {
    card.classList.add("play");
    for (const el of card.querySelectorAll(".cells i.on, .tr.on, .verdict.on")) el.classList.remove("on");
  };
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
    for (const cell of card.querySelectorAll(".cells i.on")) cell.dataset.on = "1";
    motions.set(card, { play: () => playRuns(card), reset: () => resetRuns(card) });
  }

  // The bars: `--w` is each bar's share; it goes to zero at the start and
  // back to its value in view, which the stylesheet transitions, row
  // after row.
  for (const card of document.querySelectorAll(".compare")) {
    const bars = [...card.querySelectorAll(".bar")];
    bars.forEach((bar, i) => {
      bar.dataset.w = bar.style.getPropertyValue("--w");
      bar.style.setProperty("--delay", `${i * 50}ms`);
    });
    motions.set(card, {
      play: async () => {
        for (const bar of bars) bar.style.setProperty("--w", bar.dataset.w);
        await sleep(700 + bars.length * 50);
      },
      reset: () => { for (const bar of bars) bar.style.setProperty("--w", "0%"); },
    });
  }

  for (const motion of motions.values()) {
    motion.reset();
    motion.armed = true;
  }

  // The starts above are instant: the transitions are enabled only now,
  // after a reflow, so a bar already on screen does not shrink first.
  void document.body.offsetWidth;
  document.documentElement.classList.add("motion");

  // Two fifths on screen plays an armed target — or half the viewport, for
  // a target taller than the screen, read at each tenth as it scrolls in;
  // off the screen whole, a target that has played goes back to its start
  // and is armed again. A target that leaves while still playing is reset
  // when its play ends.
  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      const motion = motions.get(entry.target);
      const tall = entry.rootBounds && entry.intersectionRect.height >= entry.rootBounds.height * 0.5;
      if (entry.intersectionRatio >= 0.4 || tall) {
        if (motion.running) motion.resetWhenDone = false;   // back before it ended: let it finish and stand
        if (!motion.armed || motion.running) continue;
        motion.armed = false;
        motion.running = true;
        Promise.resolve(motion.play()).then(() => {
          motion.running = false;
          if (motion.resetWhenDone) {
            motion.resetWhenDone = false;
            motion.reset();
            motion.armed = true;
          }
        });
      } else if (!entry.isIntersecting && !motion.armed) {
        if (motion.running) motion.resetWhenDone = true;
        else {
          motion.reset();
          motion.armed = true;
        }
      }
    }
  }, { threshold: [0, 0.1, 0.2, 0.3, 0.4] });
  for (const target of motions.keys()) observer.observe(target);
}
