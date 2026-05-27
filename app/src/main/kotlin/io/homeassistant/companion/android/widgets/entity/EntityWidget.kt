package io.homeassistant.companion.android.widgets.entity

import android.app.PendingIntent
import android.appwidget.AppWidgetManager
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.graphics.Color
import android.util.TypedValue
import android.view.View
import android.widget.RemoteViews
import android.widget.Toast
import androidx.core.content.ContextCompat
import androidx.core.graphics.toColorInt
import com.google.android.material.color.DynamicColors
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import dagger.hilt.android.AndroidEntryPoint
import io.homeassistant.companion.android.R
import io.homeassistant.companion.android.common.R as commonR
import io.homeassistant.companion.android.common.data.integration.Entity
import io.homeassistant.companion.android.common.data.integration.canSupportPrecision
import io.homeassistant.companion.android.common.data.integration.friendlyState
import io.homeassistant.companion.android.common.data.integration.onEntityPressedWithoutState
import io.homeassistant.companion.android.database.widget.StaticWidgetDao
import io.homeassistant.companion.android.database.widget.StaticWidgetEntity
import io.homeassistant.companion.android.database.widget.WidgetBackgroundType
import io.homeassistant.companion.android.database.widget.WidgetTapAction
import io.homeassistant.companion.android.util.getAttribute
import io.homeassistant.companion.android.widgets.BaseWidgetProvider
import javax.inject.Inject
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import timber.log.Timber

@AndroidEntryPoint
class EntityWidget : BaseWidgetProvider<StaticWidgetEntity, StaticWidgetDao>() {

    companion object {
        internal const val TOGGLE_ENTITY =
            "io.homeassistant.companion.android.widgets.entity.EntityWidget.TOGGLE_ENTITY"

        private data class ResolvedText(val text: CharSequence?, val error: Boolean = false)

        private var widgetIOScope: CoroutineScope = newCoroutineIOScopeProvider()

        private val widgetMqttJobs = mutableMapOf<Int, Job>()

        private fun newCoroutineIOScopeProvider() = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    @Inject
    lateinit var mqttClient: Mqtt5AsyncClient

    private val widgetIOScope
        get() = Companion.widgetIOScope

    init {
        setupWidgetScope()
    }

    private fun setupWidgetScope() {
        if (!widgetIOScope.isActive) {
            Companion.widgetIOScope = newCoroutineIOScopeProvider()
        }
    }


    override fun getWidgetProvider(context: Context): ComponentName = ComponentName(context, EntityWidget::class.java)

    override suspend fun onScreenOn(context: Context) {
        Timber.d("onScreenOn")
        super.onScreenOn(context)

        val allWidgets = getAllWidgetIdsWithEntities(context)
        val widgetsWithMqttTopic = allWidgets.filter { !getWidgetMqttTopic(it.key).isNullOrEmpty() }
        Timber.d("found ${widgetsWithMqttTopic.size} mqtt widget(s)")
        if (widgetsWithMqttTopic.isNotEmpty()) {
            Timber.d("checking if mqtt is connected")
            if (!mqttClient.state.isConnected) {
                Timber.d("no.. connecting to mqtt server ${mqttClient.config.serverHost}")
                mqttClient.connect().await()
            } else {
                Timber.d("yes")
            }
            Timber.d("connected: ${mqttClient.state.isConnected}")

            widgetsWithMqttTopic.forEach { (id, _) ->
                widgetMqttJobs[id]?.cancel()
                getWidgetMqttTopic(id)?.let { topic ->
                    widgetMqttJobs[id] = widgetIOScope.launch {
                        Timber.d("[$id] subscribing to topic: $topic")
                        mqttClient.subscribeWith()
                            .topicFilter(topic)
                            .qos(MqttQos.AT_LEAST_ONCE)
                            .callback { msg ->
                                try {
                                    Timber.d("[$id] received mqtt message ${String(msg.payloadAsBytes)}")
                                    onMqttMessage(context, id, String(msg.payloadAsBytes))
                                } catch (e: Exception) {
                                    Timber.e(e)
                                }
                            }
                            .send()
                            .await()
                        try {
                            while (isActive) {
                                Timber.d("[$id] waiting for mqtt messages")
                                delay(5000)
                            }
                            awaitCancellation()
                        } finally {
                            Timber.d("[$id] thread cancelled, bye")
                        }
                    }
                }
            }
        }
    }

    private suspend fun getWidgetMqttTopic(appWidgetId: Int): String? {
        return dao.get(appWidgetId)?.let { widget ->
            val entity = serverManager.integrationRepository(widget.serverId).getEntity(widget.entityId)
            return entity?.attributes?.get("mqtt_topic") as? String
        }
    }

    private fun onMqttMessage(context: Context, appWidgetId: Int, message: String) {
        widgetScope.launch {
            dao.updateWidgetLastUpdate(
                appWidgetId,
                message,
            )
            dao.get(appWidgetId)?.apply {
                val useDynamicColors =
                    backgroundType == WidgetBackgroundType.DYNAMICCOLOR && DynamicColors.isDynamicColorAvailable()
                val views = RemoteViews(
                    context.packageName,
                    if (useDynamicColors) R.layout.widget_static_wrapper_dynamiccolor else R.layout.widget_static_wrapper_default,
                ).apply {
                    setTextViewText(
                        R.id.widgetText,
                        ResolvedText(message).text,
                    )
                }
                AppWidgetManager.getInstance(context).partiallyUpdateAppWidget(appWidgetId, views)
            }
        }
    }

    override fun onScreenOff() {
        try {
            widgetIOScope.cancel()
        } catch (e: IllegalStateException) {
            Timber.w(e, "Calling onScreenOff without any job started")
        }
        super.onScreenOff()
    }

    override suspend fun getWidgetRemoteViews(
        context: Context,
        appWidgetId: Int,
        suggestedEntity: Entity?,
    ): RemoteViews {
        val widget = dao.get(appWidgetId)

        val intent = Intent(context, EntityWidget::class.java).apply {
            action = if (widget?.tapAction == WidgetTapAction.TOGGLE) TOGGLE_ENTITY else UPDATE_VIEW
            putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, appWidgetId)
        }

        val useDynamicColors =
            widget?.backgroundType == WidgetBackgroundType.DYNAMICCOLOR && DynamicColors.isDynamicColorAvailable()
        val views = RemoteViews(
            context.packageName,
            if (useDynamicColors) {
                R.layout.widget_static_wrapper_dynamiccolor
            } else {
                R.layout.widget_static_wrapper_default
            },
        ).apply {
            if (widget != null) {
                val serverId = widget.serverId
                val entityId: String = widget.entityId
                val attributeIds: String? = widget.attributeIds
                val label: String? = widget.label
                val textSize: Float = widget.textSize
                val stateSeparator: String = widget.stateSeparator
                val attributeSeparator: String = widget.attributeSeparator

                // Theming
                if (widget.backgroundType == WidgetBackgroundType.TRANSPARENT) {
                    var textColor = context.getAttribute(
                        R.attr.colorWidgetOnBackground,
                        ContextCompat.getColor(context, commonR.color.colorWidgetButtonLabel),
                    )
                    widget.textColor?.let { textColor = it.toColorInt() }

                    setInt(R.id.widgetLayout, "setBackgroundColor", Color.TRANSPARENT)
                    setTextColor(R.id.widgetText, textColor)
                    setTextColor(R.id.widgetLabel, textColor)
                }

                // Content
                setViewVisibility(
                    R.id.widgetTextLayout,
                    View.VISIBLE,
                )
                setViewVisibility(
                    R.id.widgetProgressBar,
                    View.INVISIBLE,
                )
                val resolvedText = resolveTextToShow(
                    context,
                    serverId,
                    entityId,
                    suggestedEntity,
                    attributeIds,
                    stateSeparator,
                    attributeSeparator,
                    appWidgetId,
                )
                setTextViewTextSize(
                    R.id.widgetText,
                    TypedValue.COMPLEX_UNIT_SP,
                    textSize,
                )
                setTextViewText(
                    R.id.widgetText,
                    resolvedText.text,
                )
                setTextViewText(
                    R.id.widgetLabel,
                    label ?: entityId,
                )
                setViewVisibility(
                    R.id.widgetStaticError,
                    if (resolvedText.error) View.VISIBLE else View.GONE,
                )
                setOnClickPendingIntent(
                    R.id.widgetTextLayout,
                    PendingIntent.getBroadcast(
                        context,
                        appWidgetId,
                        intent,
                        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
                    ),
                )
            } else {
                setTextViewText(R.id.widgetText, "")
                setTextViewText(R.id.widgetLabel, "")
            }
        }

        return views
    }

    override suspend fun getAllWidgetIdsWithEntities(context: Context): Map<Int, Pair<Int, List<String>>> =
        dao.getAll().associate { it.id to (it.serverId to listOf(it.entityId)) }

    private suspend fun resolveTextToShow(
        context: Context,
        serverId: Int,
        entityId: String?,
        suggestedEntity: Entity?,
        attributeIds: String?,
        stateSeparator: String,
        attributeSeparator: String,
        appWidgetId: Int,
    ): ResolvedText {
        var entity: Entity? = null
        try {
            entity = if (suggestedEntity != null && suggestedEntity.entityId == entityId) {
                suggestedEntity
            } else {
                entityId?.let { serverManager.integrationRepository(serverId).getEntity(it) }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Timber.e(e, "Unable to fetch entity")
        }
        val entityOptions = if (
            entity?.canSupportPrecision() == true &&
            serverManager.getServer(serverId)?.version?.isAtLeast(2023, 3) == true
        ) {
            try {
                serverManager.webSocketRepository(serverId).getEntityRegistryFor(entity.entityId)?.options
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Timber.e(e, "Failed to get options")
                null
            }
        } else {
            null
        }

        if (entity == null) {
            return ResolvedText(dao.get(appWidgetId)?.lastUpdate, true)
        }

        if (attributeIds == null) {
            val lastUpdate = entity.friendlyState(context, entityOptions)
            dao.updateWidgetLastUpdate(
                appWidgetId,
                lastUpdate,
            )
            return ResolvedText(lastUpdate)
        }

        try {
            val fetchedAttributes = entity.attributes as? Map<*, *> ?: mapOf<String, String>()
            val attributeValues =
                attributeIds.split(",").mapNotNull { id -> fetchedAttributes[id]?.toString() }
            val lastUpdate =
                entity.friendlyState(
                    context,
                    entityOptions,
                ).plus(if (attributeValues.isNotEmpty()) stateSeparator else "")
                    .plus(attributeValues.joinToString(attributeSeparator))
            dao.updateWidgetLastUpdate(appWidgetId, lastUpdate)
            return ResolvedText(lastUpdate)
        } catch (e: Exception) {
            Timber.e(e, "Unable to fetch entity state and attributes")
        }
        return ResolvedText(dao.get(appWidgetId)?.lastUpdate, true)
    }

    override suspend fun onEntityStateChanged(context: Context, appWidgetId: Int, entity: Entity) {
        val views = getWidgetRemoteViews(context, appWidgetId, entity)
        AppWidgetManager.getInstance(context).updateAppWidget(appWidgetId, views)
    }

    private suspend fun toggleEntity(context: Context, appWidgetId: Int) {
        // Show progress bar as feedback
        val appWidgetManager = AppWidgetManager.getInstance(context)
        val loadingViews = RemoteViews(context.packageName, R.layout.widget_static)
        loadingViews.setViewVisibility(R.id.widgetProgressBar, View.VISIBLE)
        loadingViews.setViewVisibility(R.id.widgetTextLayout, View.GONE)
        appWidgetManager.partiallyUpdateAppWidget(appWidgetId, loadingViews)

        var success = false
        dao.get(appWidgetId)?.let {
            try {
                onEntityPressedWithoutState(
                    it.entityId,
                    serverManager.integrationRepository(it.serverId),
                )
                success = true
            } catch (e: Exception) {
                Timber.e(e, "Unable to send toggle service call")
            }
        }

        if (!success) {
            withContext(Dispatchers.Main) {
                Toast.makeText(context, commonR.string.action_failure, Toast.LENGTH_LONG).show()
            }

            val views = getWidgetRemoteViews(context, appWidgetId)
            appWidgetManager.updateAppWidget(appWidgetId, views)
        } // else update will be triggered by websocket subscription
    }

    override suspend fun onReceiveIntentNotHandled(context: Context, intent: Intent, appWidgetId: Int) {
        when (intent.action.toString()) {
            TOGGLE_ENTITY -> toggleEntity(context, appWidgetId)
        }
    }

    override fun onDeleted(context: Context, appWidgetIds: IntArray) {
        widgetScope.launch {
            dao.deleteAll(appWidgetIds)
            appWidgetIds.forEach { removeSubscription(it) }
        }
    }
}
