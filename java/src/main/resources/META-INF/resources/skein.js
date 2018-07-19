"use strict";

function containerTable(name, containers) {
  const table = $(
    `<table class="table">
         <thead>
              <tr>
                  <th scope="col">#</th>
                  <th scope="col">Container ID</th>
                  <th scope="col">State</th>
                  <th scope="col">Log</th>
              </tr>
         </thead>
         <tbody></tbody>
     </table>`)

  containers.forEach((container, idx) => {
    const baseLogUri = (
      container.yarnNodeHttpAddress
      + "/node/containerlogs/"
      + container.yarnContainerId
      + "/user/"  // Is the user validated?
      + name + ".log?start=0"
    );

    table.find("tbody").append(
      `<tr>
           <th scope="row">${idx}</th>
           <td>${container.yarnContainerId}</td>
           <td>${container.state}</td>
           <td><a href="//${baseLogUri}">${name}.log</a></td>
      </tr>`)
  });

  return table;
};

$(() => {
  $.get("api/application", (id) => $("#application").text(id));
  $.getJSON("api/containers", (services) => {
    const tabBar = $("#service-tabs");
    const tabContents = $("#service-containers");
    for (const [name, containers] of Object.entries(services).sort()) {
      tabBar.append(
        $(`<li class="nav-item"><a class="nav-link" data-toggle="tab" href="#${name}">${name}</a>`));
      tabContents.append(
        $(`<div class="tab-pane" id="${name}">`).append(containerTable(name, containers)));
    }

    $("#service-tabs li:first-child a").tab("show")
  });
});
