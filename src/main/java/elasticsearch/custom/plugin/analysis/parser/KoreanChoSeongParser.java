package elasticsearch.custom.plugin.analysis.parser;

import elasticsearch.custom.plugin.analysis.dto.Korean;
import elasticsearch.custom.plugin.common.util.JamoUtil;

import java.util.List;

public class KoreanChoSeongParser extends AbstractKoreanParser<String> {
    private final boolean removeSingleJaeum;
    private final boolean removeSingleMoeum;

    public KoreanChoSeongParser() {
        this.removeSingleJaeum = false;
        this.removeSingleMoeum = false;
    }

    public KoreanChoSeongParser(boolean removeSingleJaeum, boolean removeSingleMoeum) {
        this.removeSingleJaeum = removeSingleJaeum;
        this.removeSingleMoeum = removeSingleMoeum;
    }

    @Override
    public String parse(String token) {

        List<Korean> korean = super.parser(token);
        StringBuilder result = new StringBuilder();

        for (Korean k : korean) {
            if (k.isCombinedKorean()) {
                result.append(k.getChoSeong());
            } else {
                char otherChar = k.getOtherChar();
                int intOtherChar = (int) otherChar;
                boolean isSingleJaeum = (intOtherChar >= JamoUtil.START_SINGLE_JAEUM_KOREA_UNICODE
                        && intOtherChar <= JamoUtil.END_SINGLE_JAEUM_KOREA_UNICODE);
                boolean isSingleMoeum = (intOtherChar >= JamoUtil.START_SINGLE_MOEUM_KOREA_UNICODE
                        && intOtherChar <= JamoUtil.END_SINGLE_MOEUM_KOREA_UNICODE);

                boolean shouldAppend = (!isSingleJaeum && !isSingleMoeum) ||            // 자음, 모음 모두 아닐 경우
                                       (!this.removeSingleJaeum && isSingleJaeum) ||    // 자음이면서 자음 제거 조건이 false 일 경우
                                       (!this.removeSingleMoeum && isSingleMoeum);      // 모음이면서 모음 제거 조건이 false 일 경우
                if (shouldAppend) {
                    result.append(otherChar);
                }
            }
        }

        return result.toString();
    }

}
