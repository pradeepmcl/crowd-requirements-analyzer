package edu.ncsu.mas.platys.crowdre.analyzer.nlp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.cmu.lti.jawjaw.pobj.POS;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;

public class WordnetSimilarityComputer {

  private final ILexicalDatabase db = new NictWordNet();

  private final RelatednessCalculator rc;

  private final OpenNLPSingleton nlp;

  private final Set<String> stopWords = new HashSet<String>();

  public WordnetSimilarityComputer(String stopWordsFile) throws FileNotFoundException, IOException {
    WS4JConfiguration conf = WS4JConfiguration.getInstance();
    conf.setMFS(false);
    // conf.setStem(false);
    // conf.setStopList("stoplist.txt"); // This did not work!

    try (BufferedReader stopWordsReader = new BufferedReader(new FileReader(stopWordsFile))) {
      String line;
      while ((line = stopWordsReader.readLine()) != null) {
        stopWords.add(line);
      }
    }

    // Change this to try other measures
    rc = new WuPalmer(db);

    nlp = OpenNLPSingleton.INSTANCE;
  }

  /**
   * Returns the similarity between two sentences. The returned similarity is
   * not symmetric
   * 
   * @param sentence1
   * @param sentence2
   * @return
   */
  public double computeSentenceSimilarity(String sentence1, String sentence2) {
    String[] words1 = nlp.tokenize(sentence1);
    String[] words2 = nlp.tokenize(sentence2);

    String[] postag1 = nlp.postag(words1);
    String[] postag2 = nlp.postag(words2);

    List<Double> simScores = new ArrayList<Double>();
    for (int i = 0; i < words1.length; i++) {
      if (!stopWords.contains(words1[i])) {
        String pt1 = postag1[i];
        String w1 = MorphaStemmer.stemToken(words1[i].toLowerCase(), pt1);
        POS p1 = mapPOS(pt1);

        Double maxSim = -1.0;
        if (p1 != null) {
          for (int j = 0; j < words2.length; j++) {
            if (!stopWords.contains(words2[j])) {
              String pt2 = postag2[j];
              String w2 = MorphaStemmer.stemToken(words2[j].toLowerCase(), pt2);
              POS p2 = mapPOS(pt2);

              if (p2 != null) {
                // double dist = rc.calcRelatednessOfWords(words1[i],
                // words1[j]);
                double sim = rc.calcRelatednessOfWords(w1 + "#" + p1, w2 + "#" + p1);
                if (sim > maxSim) {
                  maxSim = sim;
                }
                System.out.println(w1 + ", " + w2 + ": " + sim);
              }
            }
          }
        }

        if (maxSim > -1.0) {
          simScores.add(maxSim);
        }
      }
    }
    System.out.println(simScores);
    return calculateMean(simScores);
  }

  private static POS mapPOS(String pennTreePosTag) {
    if (pennTreePosTag.indexOf("NN") == 0)
      return POS.n;
    if (pennTreePosTag.indexOf("VB") == 0)
      return POS.v;
    if (pennTreePosTag.indexOf("JJ") == 0)
      return POS.a;
    if (pennTreePosTag.indexOf("RB") == 0)
      return POS.r;
    return null;
  }

  private double calculateMean(List<Double> scores) {
    Double sum = 0.0;
    if (!scores.isEmpty()) {
      for (Double mark : scores) {
        sum += mark;
      }
      return sum / scores.size();
    }
    return sum;
  }

  public static void main(String[] args) throws FileNotFoundException, IOException {
    // String s1 = "Eventually, a huge cyclone hit the entrance of my house.";
    // String s2 =
    // "Finally, a massive hurricane attacked the front of my home.";

    String s1 = "lights to dim in my house";
    String s2 = "play music to cheer my mood";

    WordnetSimilarityComputer simComp = new WordnetSimilarityComputer("src/main/resources/stoplist.txt");
    System.out.println(simComp.computeSentenceSimilarity(s1, s2));
  }

}
