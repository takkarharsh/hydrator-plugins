package co.cask.format.orc.input;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.hydrator.format.input.PathTrackingConfig;
import co.cask.hydrator.format.input.PathTrackingInputFormatProvider;

import java.util.Map;

/**
 * @author Harsh Takkar
 */
@Plugin(type = "inputformat")
@Name(OrcInputFormatProvider.NAME)
@Description(OrcInputFormatProvider.DESC)
public class OrcInputFormatProvider extends PathTrackingInputFormatProvider<PathTrackingConfig> {
    static final String NAME = "orc";
    static final String DESC = "Plugin for reading files in text format.";
    public static final PluginClass PLUGIN_CLASS =
            new PluginClass("inputformat", NAME, DESC, OrcInputFormatProvider.class.getName(),
                    "conf", PathTrackingConfig.FIELDS);

    protected OrcInputFormatProvider(PathTrackingConfig conf) {
        super(conf);
    }

    @Override
    public String getInputFormatClassName() {
        return CombineOrcInputFormat.class.getName();
    }

}
