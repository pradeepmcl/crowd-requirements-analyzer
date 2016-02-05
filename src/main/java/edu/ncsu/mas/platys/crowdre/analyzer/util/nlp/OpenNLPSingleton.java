package edu.ncsu.mas.platys.crowdre.analyzer.util.nlp;

import java.io.InputStream;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public enum OpenNLPSingleton {
  INSTANCE;

  private TokenizerME tokenizer;
  private POSTaggerME posTagger;

  private OpenNLPSingleton() {
    try {
      InputStream inTokenizer = OpenNLPSingleton.class.getResourceAsStream("/opennlp/en-token.bin");
      InputStream inPOS = OpenNLPSingleton.class.getResourceAsStream("/opennlp/en-pos-maxent.bin");

      TokenizerModel modelTokenizer = new TokenizerModel(inTokenizer);
      POSModel modelPOS = new POSModel(inPOS);

      tokenizer = new TokenizerME(modelTokenizer);
      posTagger = new POSTaggerME(modelPOS);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String[] tokenize(String sentence) {
    return tokenizer.tokenize(sentence);
  }

  public String[] postag(String[] tokens) {
    return posTagger.tag(tokens);
  }
}
