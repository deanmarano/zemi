export function onRouteDidUpdate() {
  // Github buttons
  const script = document.createElement("script");
  script.setAttribute("src", "https://buttons.github.io/buttons.js");
  script.setAttribute("async", true);
  script.setAttribute("defer", true);
  document.body.appendChild(script);

  // Open external links in new tab
  const links = document.getElementsByTagName("a");
  [...links].forEach((link) => {
    if (link.hostname !== location.hostname) {
      link.target = "_blank";
    }
  });
}
