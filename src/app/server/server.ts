import { createApp, files, jobs, lakebase, server } from '@databricks/appkit';
import { setupExpedientRoutes } from './routes/expedient-routes';

createApp({
  plugins: [
    lakebase(),
    // Uploads land in the "files" volume (from DATABRICKS_VOLUME_FILES → the raw_docs volume).
    // allowAll so the SP can write; per-user restriction comes with auth hardening in Phase 6.
    files({
      volumes: {
        files: { policy: files.policy.allowAll() },
      },
    }),
    // Lets the app trigger the ingest+classify job deterministically after an upload
    // (DATABRICKS_JOB_INGEST → key "ingest"), instead of relying on the file_arrival
    // trigger, whose detection latency proved unreliable (build-plan Phase 4 carry-over).
    jobs(),
    server(),
  ],
  async onPluginsReady(appkit) {
    // The generated PluginMap satisfies the structural AppKit interface the routes use.
    await setupExpedientRoutes(appkit as unknown as Parameters<typeof setupExpedientRoutes>[0]);
  },
}).catch(console.error);
