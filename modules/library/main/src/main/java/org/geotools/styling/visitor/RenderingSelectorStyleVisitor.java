package org.geotools.styling.visitor;

import java.util.Map;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Rule;
import org.geotools.styling.Symbolizer;

public class RenderingSelectorStyleVisitor extends DuplicatingStyleVisitor {

    public static final String RENDERING_MAP_OPTION = "renderingMap";
    public static final String RENDERING_LEGEND_OPTION = "renderingLegend";

    private String renderingOption;

    public RenderingSelectorStyleVisitor(String renderingOption) {
        this.renderingOption = renderingOption;
    }

    @Override
    public void visit(FeatureTypeStyle fts) {
        if (canRender(fts.getOptions())) super.visit(fts);
    }

    @Override
    public void visit(Rule rule) {
        if (canRender(rule.getOptions())) super.visit(rule);
    }

    @Override
    public void visit(Symbolizer sym) {
        if (canRender(sym.getOptions())) super.visit(sym);
    }

    @Override
    protected Symbolizer copy(Symbolizer symbolizer) {
        if (canRender(symbolizer.getOptions())) return super.copy(symbolizer);
        else return null;
    }

    protected boolean canRender(Map<String, String> vendorOptions) {
        String value = vendorOptions != null ? vendorOptions.get(renderingOption) : null;
        return value == null || Boolean.valueOf(value).booleanValue();
    }
}
