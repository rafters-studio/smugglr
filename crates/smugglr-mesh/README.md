# smugglr-mesh

The smugglr mesh: nodes on a local network kept in step with nobody in charge. `run` runs one node until it is told to stop. A node finds its peers by broadcast or a peers setting, sends each change as it happens over UDP, sealed and signed under a shared key, and resends until every peer has acknowledged it. `new_key` makes a shared key; `remove` tells the mesh a node is gone for good; `replay` re-sends a change the node dropped.

Depend on it to run a mesh node inside your own program; `smugglr mesh` runs one from the command line.

smugglr's README: https://github.com/rafters-studio/smugglr#readme
