package edu.ncsu.mas.platys.crowdre.analyzer.util.nlp;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.languagetool.JLanguageTool;
import org.languagetool.language.AmericanEnglish;
import org.languagetool.rules.RuleMatch;

public class SpellChecker {

  private final JLanguageTool mLangTool = new JLanguageTool(new AmericanEnglish());

  public Map<Integer, List<String>> getSuggestions(String line) throws IOException {
    List<RuleMatch> matches = mLangTool.check(line);

    Map<Integer, List<String>> colToSuggestions = new LinkedHashMap<Integer, List<String>>();
    for (RuleMatch match : matches) {
      if (!match.getRule().getId().equals("UPPERCASE_SENTENCE_START")) {
        colToSuggestions.put(match.getColumn(), match.getSuggestedReplacements());
      }
    }

    return colToSuggestions;
  }

  public static void main(String[] args) throws IOException {
    JLanguageTool langTool = new JLanguageTool(new AmericanEnglish());
    List<RuleMatch> matches = langTool.check("home occupent");

    for (RuleMatch match : matches) {
      System.out.println("Potential error at line " + match.getLine() + ", column "
          + match.getColumn() + ": " + match.getMessage());
      System.out.println("Suggested correction: " + match.getSuggestedReplacements());
      System.out.println(match.getRule());
      System.out.println(match.getRule().isDictionaryBasedSpellingRule());
      System.out.println(match.getRule().getId());
      System.out.println(match.getRule().getCategory());
    }
  }
}
