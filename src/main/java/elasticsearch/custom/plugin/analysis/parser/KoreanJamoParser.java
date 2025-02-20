package elasticsearch.custom.plugin.analysis.parser;

import elasticsearch.custom.plugin.analysis.dto.Korean;
import elasticsearch.custom.plugin.common.util.JamoUtil;

import java.util.List;

public class KoreanJamoParser extends AbstractKoreanParser<String> {

    @Override
    public String parse(String token) {
        List<Korean> korean = super.parser(token);
        StringBuilder result = new StringBuilder();

        for (Korean k : korean) {
            if (k.isCombinedKorean()) {
                result.append(k.getChoSeong()).append(k.getJungSeong());
                if (k.getJongSeong() != JamoUtil.UNICODE_JONG_SEONG_EMPTY) {
                    result.append(k.getJongSeong());
                }
            } else {
                result.append(k.getOtherChar());
            }
        }

        return result.toString();
    }
}
