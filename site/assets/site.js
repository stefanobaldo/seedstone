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

// The hero animation. The SVG's markup is its final frame; this driver
// clears it and replays it: the first run ticks along, the clients send
// their commands, the assertion fails at a tick and the trace prints;
// the second run, from the same seed, does the same; the check mark.
// Under reduced motion the final frame stays as it is.
const anim = document.querySelector(".anim");
if (anim && !matchMedia("(prefers-reduced-motion: reduce)").matches) {
  const runs = anim.querySelectorAll(".run");
  const chips = anim.querySelectorAll(".chip");
  const clock = anim.querySelector(".clock");
  const verdict = anim.querySelector(".verdict");
  const TICK_MS = 70;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
      await sleep(TICK_MS);
      if (cells[tick].classList.contains("hit")) break;
    }
    lane.querySelector(".trace").classList.add("on");
  };

  // Three times through, then the picture rests on its final frame, which
  // is what the markup already is after the third verdict.
  (async () => {
    for (let round = 0; round < 3; round++) {
      clear(anim);
      clock.textContent = "t = 0.000 s";
      await sleep(600);
      await run(runs[0]);
      await sleep(1000);
      await run(runs[1]);
      await sleep(400);
      verdict.classList.add("on");
      await sleep(2000);
    }
  })();
}
