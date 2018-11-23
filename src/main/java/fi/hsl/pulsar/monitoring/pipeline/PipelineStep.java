package fi.hsl.pulsar.monitoring.pipeline;

public abstract class PipelineStep<T> {
    PipelineStep<T> parent;

    public PipelineStep() {
        this(null);
    }

    public PipelineStep(PipelineStep<T> parent) {
        this.parent = parent;
    }
    /*
    Let's use decorator pattern here. Each component in the pipeline just adds something to the context
     and doesn't have to care about other parts of the pipeline
     */
    public PipelineContext handleMessage(PipelineContext context, T msg) {
        if (parent != null) {
            context = parent.handleMessage(context, msg);
        }
        return handleInternal(context, msg);
    }

    protected abstract PipelineContext handleInternal(PipelineContext context, T msg);

}
