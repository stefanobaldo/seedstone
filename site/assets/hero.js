// The copy button: it exists only where a script runs, so the page
// without one shows the line alone.
document.documentElement.classList.add("js");

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
