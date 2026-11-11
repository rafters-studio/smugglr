# smugglr-multi

Multi keeps a person's own devices in step through blobs the app stores. `stash` makes a `Blob` of the rows changed on a device since its last stash; `retrieve` verifies blobs and applies them, in any order; `replay` re-sends a change a retrieve dropped. Where blobs are kept, the device list and the keys are the app's.

Depend on it when your app moves a person's data between their devices through storage it controls, and the devices are not online together.

smugglr's README: https://github.com/rafters-studio/smugglr#readme
