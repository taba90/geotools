package org.geotools.styling.visitor;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.styling.Description;
import org.geotools.styling.DescriptionImpl;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Rule;
import org.geotools.styling.Style;
import org.geotools.styling.StyleBuilder;
import org.geotools.styling.StyleFactory;
import org.geotools.styling.Symbolizer;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.FilterFactory2;

import java.util.Arrays;
import java.util.List;

import static org.geotools.styling.visitor.RenderingSelectorStyleVisitor.RENDERING_LEGEND_OPTION;
import static org.geotools.styling.visitor.RenderingSelectorStyleVisitor.RENDERING_MAP_OPTION;
import static org.junit.Assert.assertEquals;

public class RenderingSelectorStyleVisitorTest {

    StyleBuilder sb;
    StyleFactory sf;
    FilterFactory2 ff;

    @Before
    public void setUp() throws Exception {
        sf = CommonFactoryFinder.getStyleFactory(null);
        ff = CommonFactoryFinder.getFilterFactory2(null);
        sb = new StyleBuilder(sf, ff);
    }

    @Test
    public void testMapRenderingSelection() {
        Style oldStyle = sb.createStyle("FTSName", sf.createPolygonSymbolizer());
        oldStyle.featureTypeStyles().get(0).getOptions().put(RENDERING_MAP_OPTION,"false");

        Rule rule = sf.createRule();
        Symbolizer symb1 = sf.createLineSymbolizer(sf.getDefaultStroke(), "geometry");
        rule.symbolizers().add(symb1);
        rule.getOptions().put(RENDERING_MAP_OPTION,"false");

        Rule rule2 = sf.createRule();
        rule2.setName("Rendered rule");
        Symbolizer symb2 =
                sf.createPolygonSymbolizer(sf.getDefaultStroke(), sf.getDefaultFill(), "shape");
        rule2.symbolizers().add(symb2);
        Symbolizer symb3=sf.createPolygonSymbolizer(sf.getDefaultStroke(),sf.getDefaultFill(),"extension");
        symb3.getOptions().put(RENDERING_MAP_OPTION,"true");
        rule2.symbolizers().add(symb3);

        FeatureTypeStyle fts = sf.createFeatureTypeStyle(rule);
        fts.setName("Rendered FTS");
        fts.rules().addAll(Arrays.asList(rule,rule2));
        oldStyle.featureTypeStyles().add(fts);

        RenderingSelectorStyleVisitor selectorStyleVisitor=new RenderingSelectorStyleVisitor(RENDERING_MAP_OPTION);

        selectorStyleVisitor.visit(oldStyle);

        Style visited=(Style)selectorStyleVisitor.getCopy();
        List<FeatureTypeStyle> featureTypeStyleList=visited.featureTypeStyles();
        assertEquals(1,featureTypeStyleList.size());
        FeatureTypeStyle renderedFts=featureTypeStyleList.get(0);
        assertEquals(fts.getName(),renderedFts.getName());
        assertEquals(1,renderedFts.rules().size());
        Rule renderedRule=renderedFts.rules().get(0);
        assertEquals(rule2,renderedRule);

    }


    @Test
    public void testLegendRenderingSelection() {
        Style oldStyle = sb.createStyle("FTSName", sf.createPolygonSymbolizer());
        oldStyle.featureTypeStyles().get(0).getOptions().put(RENDERING_LEGEND_OPTION,"true");

        Rule rule = sf.createRule();
        Symbolizer symb1 = sf.createLineSymbolizer(sf.getDefaultStroke(), "geometry");
        rule.symbolizers().add(symb1);
        rule.getOptions().put(RENDERING_LEGEND_OPTION,"false");

        Rule rule2 = sf.createRule();
        Symbolizer symb2 =
                sf.createPolygonSymbolizer(sf.getDefaultStroke(), sf.getDefaultFill(), "shape");
        rule2.symbolizers().add(symb2);
        Symbolizer symb3=sf.createPolygonSymbolizer(sf.getDefaultStroke(),sf.getDefaultFill(),"extension");
        symb3.getOptions().put(RENDERING_LEGEND_OPTION,"false");
        rule2.symbolizers().add(symb3);

        FeatureTypeStyle fts = sf.createFeatureTypeStyle(rule);
        fts.rules().addAll(Arrays.asList(rule,rule2));
        oldStyle.featureTypeStyles().add(fts);

        RenderingSelectorStyleVisitor selectorStyleVisitor=new RenderingSelectorStyleVisitor(RENDERING_LEGEND_OPTION);

        selectorStyleVisitor.visit(oldStyle);

        Style visited=(Style)selectorStyleVisitor.getCopy();
        List<FeatureTypeStyle> featureTypeStyleList=visited.featureTypeStyles();
        assertEquals(2,featureTypeStyleList.size());
        FeatureTypeStyle renderedFts=featureTypeStyleList.get(1);
        assertEquals(1,renderedFts.rules().size());
        Rule renderedRule=renderedFts.rules().get(0);
        assertEquals(1,renderedRule.symbolizers().size());
        Symbolizer symbolizer=renderedRule.symbolizers().get(0);
        assertEquals(symb2,symbolizer);
    }
}
