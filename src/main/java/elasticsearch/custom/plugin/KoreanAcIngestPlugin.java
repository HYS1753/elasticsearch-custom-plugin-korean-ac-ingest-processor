package elasticsearch.custom.plugin;

import elasticsearch.custom.plugin.processor.KoreanAcIngestProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

public class KoreanAcIngestPlugin extends Plugin implements IngestPlugin {
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(KoreanAcIngestProcessor.TYPE, new KoreanAcIngestProcessor.Factory());
    }
}
