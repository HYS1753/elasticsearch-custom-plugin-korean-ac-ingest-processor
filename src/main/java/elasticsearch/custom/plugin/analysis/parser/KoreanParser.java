package elasticsearch.custom.plugin.analysis.parser;

import elasticsearch.custom.plugin.analysis.dto.Korean;

import java.util.List;

public class KoreanParser extends AbstractKoreanParser<List<Korean>> {

    @Override
    public List<Korean> parse(String token) {
        return super.parser(token);
    }
}
