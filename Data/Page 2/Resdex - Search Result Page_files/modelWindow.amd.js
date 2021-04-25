define(['exports'], function (exports) { 'use strict';

  var actionNameMap = {
      modalOpen: "modalOpen"
  };

  var createActionUrl = function createActionUrl(currentRoute, actionName) {
      var url = void 0;
      if (currentRoute.indexOf('?') != -1) {
          url = currentRoute + "&action=" + actionName;
      } else {
          url = currentRoute + "?action=" + actionName;
      }
      return url;
  };

  var getModalOpenActionUrl = function getModalOpenActionUrl() {
      return createActionUrl("" + document.location.pathname + document.location.search, actionNameMap.modalOpen);
  };

  var cleanQueryString = function cleanQueryString(queryString) {
      return queryString.replace(/[&]?action=(drawerOpen|modalOpen)[&]?/g, "").replace(/^[?]$/, "");
  };

  var page = function function_name(argument) {
      // body...
  };

  var pageAlias = window.interfaces && window.interfaces.page || window.page || page;

  var asyncGenerator = function () {
    function AwaitValue(value) {
      this.value = value;
    }

    function AsyncGenerator(gen) {
      var front, back;

      function send(key, arg) {
        return new Promise(function (resolve, reject) {
          var request = {
            key: key,
            arg: arg,
            resolve: resolve,
            reject: reject,
            next: null
          };

          if (back) {
            back = back.next = request;
          } else {
            front = back = request;
            resume(key, arg);
          }
        });
      }

      function resume(key, arg) {
        try {
          var result = gen[key](arg);
          var value = result.value;

          if (value instanceof AwaitValue) {
            Promise.resolve(value.value).then(function (arg) {
              resume("next", arg);
            }, function (arg) {
              resume("throw", arg);
            });
          } else {
            settle(result.done ? "return" : "normal", result.value);
          }
        } catch (err) {
          settle("throw", err);
        }
      }

      function settle(type, value) {
        switch (type) {
          case "return":
            front.resolve({
              value: value,
              done: true
            });
            break;

          case "throw":
            front.reject(value);
            break;

          default:
            front.resolve({
              value: value,
              done: false
            });
            break;
        }

        front = front.next;

        if (front) {
          resume(front.key, front.arg);
        } else {
          back = null;
        }
      }

      this._invoke = send;

      if (typeof gen.return !== "function") {
        this.return = undefined;
      }
    }

    if (typeof Symbol === "function" && Symbol.asyncIterator) {
      AsyncGenerator.prototype[Symbol.asyncIterator] = function () {
        return this;
      };
    }

    AsyncGenerator.prototype.next = function (arg) {
      return this._invoke("next", arg);
    };

    AsyncGenerator.prototype.throw = function (arg) {
      return this._invoke("throw", arg);
    };

    AsyncGenerator.prototype.return = function (arg) {
      return this._invoke("return", arg);
    };

    return {
      wrap: function (fn) {
        return function () {
          return new AsyncGenerator(fn.apply(this, arguments));
        };
      },
      await: function (value) {
        return new AwaitValue(value);
      }
    };
  }();















  var _extends = Object.assign || function (target) {
    for (var i = 1; i < arguments.length; i++) {
      var source = arguments[i];

      for (var key in source) {
        if (Object.prototype.hasOwnProperty.call(source, key)) {
          target[key] = source[key];
        }
      }
    }

    return target;
  };

  var stack = [];
  //Close success resolved callback, when modal is closed by internal controls.
  var interalClosePromiseResolve = null;
  //Counter to detect  route change to action=drawerOpen in case of back button press or new route
  var modalSlideCount = -1;
  //Direction variable "forward| backward"
  var direction = "forward";
  //Internal state
  var previousState = { route: null };

  /**
   * [Function to handle modal close after route change]
   * @return {[type]} [description]
   */
  var closeModalOnBrowserBack = function closeModalOnBrowserBack() {
      /**
       * Will be a new promise if modal close by internal actions
       * Will be null otherwize
       */
      if (interalClosePromiseResolve) {
          interalClosePromiseResolve();
          interalClosePromiseResolve = null;
      } else if (stack.length) {
          //Modal closed through browser back button
          stack.shift().close();
      }
  };

  /**
   * [Function to detect browser back or forward ]
   * @param  {[type]} ctx [description]
   * @return {[type]}     [description]
   */
  var getRouteDirection = function getRouteDirection(ctx) {
      if (ctx.state.modalSlideCount != undefined) {
          direction = modalSlideCount < ctx.state.modalSlideCount ? "forward" : "backward";
          modalSlideCount = ctx.state.modalSlideCount;
      } else {
          direction = "forward";
          setTimeout(function () {
              ctx.state.modalSlideCount = ++modalSlideCount;
              ctx.save();
          }, 0);
      }
      return direction;
  };

  /**
   * [
      * Adds route handler for modal
      * If route containes ?action=modalOpen they don't fire route change
      * If route on browser back contains ?action=modalOpen then close topmost modal.
      ]
   */
  var addRouteHandler = function addRouteHandler() {
      pageAlias("*", function (ctx, next) {
          if (ctx.querystring.match(/[&?]*action=modalOpen/)) {
              if (getRouteDirection(ctx) == "backward") {
                  closeModalOnBrowserBack();
              }
          } else {
              next();
          }
      });
  };

  /**
   * [
      * onOpen is called in open.success, if options.routing is true
      * It pushes new state in history on opening lightbox.
      ]
   *       
   * @param  {[type]} instance [description]
   * @return {[type]}          [description]
   */
  var onOpen = function onOpen(instance) {
      setTimeout(function () {
          var modalOpenActionUrl = getModalOpenActionUrl();
          if (instance.options.close && instance.options.close.replaceRoute) {

              var modalRedirectActionUrl = instance.options.close.replaceRoute;

              //Replace current route with replaceRoute
              pageAlias.redirect(modalRedirectActionUrl);
              /**
               * Single digit 0-9ms timeout is not behaving properly, i.e why for safe side 100ms timeout
               * @param  {[type]} ( [description]
               * @return {[type]}   [description]
               */
              setTimeout(function () {
                  //Redirect to <pathname>?action=modalOpen 
                  pageAlias(modalOpenActionUrl);
                  stack.unshift(instance);
              }, 100);
          } else {
              //Redirect to <pathname>?action=modalOpen 
              pageAlias(modalOpenActionUrl);
              stack.unshift(instance);
          }
      }, 0);
  };

  /**
   * [Extend modal open and success callbacks to handle browser back]
   * @param  {[type]} options [description]
   * @return {[type]}         [description]
   */
  var extendOptions = function extendOptions(options) {
      return _extends({}, options, {
          open: _extends({}, options.open, {
              success: function success() {
                  if (options.open && options.open.success) {
                      var _options$open$success;

                      (_options$open$success = options.open.success).call.apply(_options$open$success, [this].concat(Array.prototype.slice.call(arguments)));
                  }
                  onOpen(this);
              }
          }),
          close: _extends({}, options.close, {
              success: function success(close_target) {
                  var _this = this;

                  //If closed through nodes, layer or esc
                  if (close_target) {
                      var internalClosePromise = new Promise(function (resolve, reject) {
                          interalClosePromiseResolve = resolve;
                          stack.shift();
                          window.history.back();
                      });
                      //Promise will be resolved on route change
                      internalClosePromise.then(function (successMessage) {
                          if (options.close && options.close.success) {
                              options.close.success.call(_this, close_target);
                          }
                      });
                  } else {
                      if (options.close && options.close.success) {
                          options.close.success.call(this, close_target);
                      }
                  }
              }
          })

      });
  };

  /**
   * [If current route is not modal, close the last opened modal.]
   * @param  {[type]} store [description]
   * @return {[type]}       [description]
   */
  var subscribeStore = function subscribeStore(store) {
      store.subscribe(function () {
          var state = store.getState();
          if (state.route != previousState.route) {
              if (state.route.back) {
                  //If any modal is open with options.routing:true
                  closeModalOnBrowserBack();
              }
              previousState.route = state.route;
          }
      });
  };

  /**
   * [isSmallDevice Devices which do not have tabbin in keypad]
   * @type {Boolean}
   */
  var isSmallDevice = window.screen.width <= 767; //Portrait and lanscape mode phones

  function visible(element) {
      return $.expr.filters.visible(element) && !$(element).parents().addBack().filter(function () {
          return $.css(this, "visibility") === "hidden";
      }).length;
  }

  /** Refered : JQuery UI Code */
  /** Detects focusable element */
  function focusable(element, isTabIndexNotNaN) {
      var map,
          mapName,
          img,
          nodeName = element.nodeName.toLowerCase();
      if ("area" === nodeName) {
          map = element.parentNode;
          mapName = map.name;
          if (!element.href || !mapName || map.nodeName.toLowerCase() !== "map") {
              return false;
          }
          img = $("img[usemap=#" + mapName + "]")[0];
          return !!img && visible(img);
      }
      return (/input|select|textarea|button|object/.test(nodeName) ? !element.disabled : "a" === nodeName ? element.href || isTabIndexNotNaN : isTabIndexNotNaN) &&
      // the element and all of its ancestors must be visible
      visible(element);
  }

  /** Refered : JQuery UI Code */

  /** Runs once */
  var init$1 = function init() {
      var cont = $('<div class="ltCont close"></div>').attr('tabIndex', 0);
      var layer = $('<div class="ltLayer close"></div>');
      var stack = [];
      cont.append(layer);

      //Reset lightBox on resize
      $(window).bind('resize', function () {
          if (stack.length) {
              var top = stack[0];
              top.resize();
          }
      });

      //Close lightBox on close
      $('html').keydown(function (event) {
          if (event.keyCode === 27 && stack.length && stack[0].options.close.esc) {
              stack[0].close_target = event;
              stack[0].close();
          }
      });

      //Close lightBox on layer click
      layer.click(function (e) {
          e.stopPropagation();
          stack[0].close_target = this;
          if (stack.length && stack[0].options.close.layer) {
              stack[0].close();
          }
      });

      if (!isSmallDevice) {
          if (!$.expr[":"].focusable) {
              $.expr[":"].focusable = function (element) {
                  return focusable(element, !isNaN($.attr(element, "tabindex")));
              };
          }
          /** If tab is pressend on or in lightbox */
          cont.keydown(function (event) {
              if (!stack.length || event.keyCode !== 9) {
                  return;
              }
              var list = stack[0].options.model.find(':focusable');
              var first = list.first();
              var last = list.last();

              if ((event.target === last[0] || event.target === event.currentTarget) && !event.shiftKey) {
                  first ? first.focus() : '';
                  return false;
              } else if ((event.target === first[0] || event.target === event.currentTarget) && event.shiftKey) {
                  last ? last.focus() : '';
                  return false;
              }
              event.stopPropagation();
          });
      }

      /** If tab is pressed on first or last focusable element and lightbox is open */
      $('html').keydown(function (event) {
          if (event.keyCode !== 9) {
              return;
          }
          if (!isSmallDevice) {
              /** If lightbox is open */
              if (stack.length) {
                  cont.focus();
                  return false;
              }
          }
      });

      //Document Ready
      $(function () {
          $('body').append(cont);
      });

      return {
          //Top most visible lightbox is stack[0]
          stack: stack,
          cont: cont,
          layer: layer
      };
  };

  /**
   * Version : v5
   * Author : Ankit Anand
   * Desciption : 
   * Adding support to handle routes        
   * 
   */

  // JavaScript Document

  var isTransitionEndSupported = function () {

      var transEndEventNames = {
          WebkitTransition: 'webkitTransitionEnd',
          MozTransition: 'transitionend',
          OTransition: 'oTransitionEnd otransitionend',
          transition: 'transitionend'
      };
      for (var name in transEndEventNames) {
          if (document.documentElement.style[name] !== undefined) {
              return transEndEventNames[name];
          }
      }
      return null;
  }();

  /*if (DEBUG) {
      window.isTransitionEndSupported = isTransitionEndSupported;
  }
  */

  /** Some common Utility functions*/
  var util = {
      is_options_valid: function is_options_valid(options) {
          if (!options || !options.model || options.model.prop('nodeType') !== 1) {
              return false;
          }
          return true;
      },
      switchClass: function switchClass(a, b) {
          if (a) {
              this.removeClass(a);
          }
          if (b) {
              this.addClass(b);
          }
      }
  };

  var raf_switchClass = function raf_switchClass(a, b) {
      var _this2 = this;

      window.requestAnimationFrame(function () {
          util.switchClass.call(_this2, a, b);
      });
  };

  /*if (DEBUG) {
      window.util = util;
  }
  */

  var default_opt = {
      trigger: null,
      routing: true,
      model: null,
      dir: "",
      dimens: {
          height: 'auto',
          width: 'auto'
      },
      resetForm: false,
      fixed: false,
      zLayer: true,
      open: {
          minZIndex: 999,
          success: function success() {},
          event: 'click',
          selector: null,
          anim: ''
          //          anim : {className:''}               
      },
      close: {
          esc: true,
          layer: true,
          nodes: {
              target: '',
              event: 'click',
              selector: ''
          },
          success: function success() {},
          returnFocus: true,
          anim: '',
          replaceRoute: ""
      }
  };

  //lt is modalContainer singleton object
  var lt = null;
  //for findOut max ZIndex on page
  // * is replaced with .ltLayer as multiple ltCont are created when lightBox.js is included more than once. 
  function getMaxZIndex() {
      var zIndexMax = this.options.open.minZIndex;
      $('.ltCont .ltLayer').each(function () {
          var z = parseInt($(this).css('z-index'));
          if (z > zIndexMax) zIndexMax = z;
      });
      return zIndexMax;
  }

  var init_Dimensions = function init_Dimensions() {
      this.options.model.css({
          position: this.options.fixed ? 'fixed' : 'absolute',
          width: this.options.dimens.width,
          height: this.options.dimens.height
      });
  };

  var init_options = function init_options(param) {
      this.options = $.extend(true, {}, this.default_opt, param);
  };

  var init_structure = function init_structure() {
      var parent = this.options.model.parent();
      /** Check if lightBox alerady present in ltCont */
      if (!parent.hasClass('ltCont')) {
          this.options.model.addClass(this.options.dir);
          lt.cont.append(this.options.model);
      }
  };

  var animate = {
      open: function open() {
          var arr = [this.options.close.anim, this.options.open.anim];
          raf_switchClass.apply(this.options.model, arr);
      },
      close: function close(obj) {
          var arr = [this.options.open.anim, this.options.close.anim];
          raf_switchClass.apply(this.options.model, arr);
      }
  };

  var open_firstFocus = function open_firstFocus() {
      /** IE specific fix, for scroll move on focus*/
      var scrollTop = document.documentElement.scrollTop;
      var ff = this.options.open.firstFocus;
      if (ff) ff.focus();else lt.cont.focus();
      /** IE specific fix, for scroll move on focus*/
      document.documentElement.scrollTop = scrollTop;
  };
  var resetForm = function resetForm() {
      if (!this.options.resetForm) return;
      var forms = this.options.model.find('form');
      for (var key = 0; key < forms.length; key++) {
          forms[key].reset();
      }
  };

  var close_returnFocus = function close_returnFocus() {
      if (!lt.stack.length) {
          var rf = this.options.close.returnFocus;
          rf === true ? this.options.trigger.focus() : $(rf).focus();
      }
  };

  var closeTransEnd_cb = function closeTransEnd_cb() {
      if (this.transEndCbFired == false) {
          this.transEndCbFired = true;
          this.options.model.removeClass('model_open');

          //this.options.model.css('zIndex', '-1');


          if (lt.stack.length) {
              var top = lt.stack[0];
              if (this.options.zLayer) {
                  lt.layer.css('zIndex', top.options.model.css('zIndex'));
              }
          } else {
              //Last close
              lt.cont.addClass('close');
          }

          close_returnFocus.call(this);

          resetForm.call(this);

          this.options.close.success.call(this, this.close_target);
      }
  };

  var init_openclose_Properties = function init_openclose_Properties() {
      var _this = this;
      _this.state = "close";
      /** Adding event on trigger */

      this.openEventHandler = function () {
          _this.open();
      };

      this.options.trigger.on(this.options.open.event + '.' + this.pluginName, this.options.open.selector, this.openEventHandler);

      /**Adding events Closing nodes */
      init_closeNodes.call(this, this.options.close.nodes);
  };

  var init_closeNodes = function init_closeNodes(nodes) {
      var _this = this;

      if ($.type(nodes) === 'array') {
          //Recursive
          $.each(nodes, function (a, b) {
              init_closeNodes.call(_this, b);
          });
          return;
      }

      var config = null;
      if (nodes.constructor === jQuery) {
          config = $.extend({}, this.default_opt.close.nodes);
          config.target = nodes;
      } else {
          config = {
              target: $(nodes.target),
              event: nodes.event,
              selector: nodes.selector
          };
      }
      config.target.on(config.event, config.selector, function () {
          _this.close();
          _this.close_target = this;
      });
  };

  /** Run every time a new lightbox is created */
  var init = function init(param) {

      init_options.call(this, param);
      this.init_structure();
      init_Dimensions.call(this);
      init_openclose_Properties.call(this);

      this.options.model.addClass(this.options.close.anim);
      if (this.options.routing) {
          this.options = extendOptions(this.options);
      }
  };
  var reInit = function reInit(pluginName) {
      this.options.trigger.off(this.options.open.event + '.' + this.pluginName);
  };

  var off = function off() {
      this.options.trigger.off(this.options.open.event, this.openEventHandler);
  };

  var on = function on() {
      this.options.trigger.on(this.options.open.event, this.openEventHandler);
  };

  /** To be overrited */
  var resize = function resize() {
      lt.cont.css({
          width: 'auto',
          height: 'auto'
      });

      var totalH = $(document).height(),
          totalW = $(document).width();

      lt.cont.css({
          width: totalW + 'px',
          height: totalH + 'px'
      });
  };

  var open = function open() {
      this.close_target = null;
      this.transEndCbFired = false;
      this.state = 'open';
      this.options.model.addClass('model_open');
      this.options.model.data('model', this);

      /** Setting zIndex of lightBox and black layer */
      var maxZIndElm = getMaxZIndex.call(this);
      if (this.options.zLayer) {
          lt.layer.css('zIndex', maxZIndElm + 3);
      }
      this.options.model.css('zIndex', maxZIndElm + 3);

      if (!lt.stack.length) {
          //First open
          lt.cont.css('zIndex', maxZIndElm + 1);
          raf_switchClass.call(lt.layer, 'close', 'open');
          lt.cont.removeClass('close');
      }

      resetForm.call(this);

      /** Stack Update */
      if ($.inArray(this, lt.stack) === -1) {
          lt.stack.unshift(this);
      }

      /** Center align LightBox */
      this.resize();

      /** Animation Code */
      animate.open.call(this);
      /** Focus Element */
      open_firstFocus.call(this);

      /** Success callback */
      this.options.open.success.call(this);
  };

  var close = function close(index, noAnim) {

      index = index || $(lt.stack).index(this);
      if (!lt.stack.length || index < 0) return;

      this.state = 'close';
      this.options.model.data('model', this);

      lt.stack.splice(index, 1);

      if (!isTransitionEndSupported || noAnim || !this.options.open.anim) {
          closeTransEnd_cb.call(this);
      } else {

          animate.close.call(this);
      }

      if (!lt.stack.length) raf_switchClass.call(lt.layer, 'open', 'close');
  };

  var getNewModel = function getNewModel(pluginName) {

      var _const = function _const() {};
      _const.prototype = model;

      var _proto_subModel = new _const();
      _proto_subModel.pluginName = pluginName;

      function _const_subModel(options, trigger) {
          this.close_target = null;

          /** Cleaning registered events for same trigger-lightBox*/
          var obj = trigger.data('model');
          if (obj && obj.options && obj.options.model[0] === options.model[0]) {

              reInit.call(obj);
          }
          options.trigger = trigger;
          init.call(this, options);
      }
      _const_subModel.prototype = _proto_subModel;

      return _const_subModel;
  };

  /**
   * [destroy should be called in modal's detached callback]
   * @return {[type]} [description]
   */
  var destroy = function destroy() {

      this.off();
      close.call(this, undefined, true);
      if (!lt.stack.length) {
          raf_switchClass.call(lt.layer, 'open', 'close');
          lt.cont.addClass('close');
      }
      this.options.model.remove();
  };

  /**
   * [
   * Initialize modal container, layer and stack
   * If store is provided
   * Subcribe to store
   * Add route change handler
   * ]
   * @param  {[type]} store [Redux create store object]
   * @return {[type]}       [description]
   */
  var modelSetup = function modelSetup() {
      var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : { store: null };

      if (!lt) {
          lt = model.lt = init$1();
      }
      if (options.store) {
          subscribeStore(options.store);
          addRouteHandler();
      }
  };

  var model = {
      isTransitionEndSupported: isTransitionEndSupported,
      transEndCbFired: false,
      pluginName: '',
      state: 'close',
      util: util,
      lt: lt,
      reInit: reInit,
      close_returnFocus: close_returnFocus,
      getNewModel: getNewModel,
      open: open,
      close: close,
      on: on,
      off: off,
      resize: resize,
      default_opt: default_opt,
      init_structure: init_structure,
      closeTransEnd_cb: closeTransEnd_cb,
      destroy: destroy
  };

  /**
   * Version : v5
   * Desciption : Change to es6 import
   * Author : Ankit Anand
   */

  var $$1 = window.jQuery || window.Zepto;
  var pluginName = 'lightBox';

  var lightBoxModal = null;

  /**
   * [description]
   * @return {[type]} [lightBoxModal singleton object]
   */
  var getModalConstructor = function getModalConstructor() {
      if (!lightBoxModal) {

          lightBoxModal = getNewModel(pluginName);

          lightBoxModal.prototype.resize = function () {

              resize();

              var _doc = document.documentElement || document.body,
                  innerH = $$1(window).height(),
                  innerW = $$1(window).width(),
                  scrollT = window.pageYOffset ? pageYOffset : _doc.scrollTop;
              var availH = innerH - this.options.model.height(),
                  tpPos = 0;
              var availW = innerW - this.options.model.width(),
                  lpPos = 0;
              if (availH > 0) {
                  tpPos = (!this.options.fixed ? scrollT : 0) + availH / 2;
              } else {
                  /*scrollT = totalH > innerH ? scrollT - 20 : scrollT;
                  tpPos = scrollT + availH;*/
                  tpPos = 0;
                  window.scrollTo(0, tpPos);
              }
              lpPos = availW > 0 ? availW / 2 : 0;

              this.options.model.css({
                  top: tpPos + 'px',
                  left: lpPos + 'px'
              });
              return true;
          };
      }

      return lightBoxModal;
  };

  /** Commented close, closeAll and $.fn.lightbox as we are not global lightbox instances anymore */

  /*const close = function(option) {

      var start, end;

      start = 0;
      end = 0;
      option = option || {};

      if (option.all) {
          start = (model.lt.stack.length - 1);
      }
      if (option.allPrevious) {
          start = (model.lt.stack.length - 1);
          end = 1;
      } else if (option.index) {
          end = start = option.index;
      }

      for (var i = start; i >= end; i--) {
          (i in model.lt.stack) ? model.lt.stack[i].close(i, option.noAnim): '';
      }
  };
  */
  /*const closeAll = function(option) {
      $.fn.lightBox.close(option || {
          all: true
      });
  };*/

  /*$.fn.lightBox  = function(options){
      return init(options,this);
  }*/

  var lightBox = function lightBox(options) {
      var obj = null,
          mask = null;

      /** Support for legacy option : ltBox*/
      var temp;
      if (options) {
          try {
              temp = options.model;
              options.model = options.ltBox;
              options.ltBox = null;
              if (!options.model) throw temp;
          } catch (e) {
              options.model = e;
          }
      }

      if (util.is_options_valid(options)) {

          /** Support for legacy option : anim*/
          if (options.open) {
              try {
                  temp = options.open.anim;
                  if (temp && !(options.open.anim = options.open.anim.className)) throw temp;
              } catch (e) {
                  options.open.anim = e;
              }
          }

          if (options.close) {
              try {
                  temp = options.close.anim;
                  if (temp && !(options.close.anim = options.close.anim.className)) throw temp;
              } catch (e) {
                  options.close.anim = e;
              }
          }

          var modalConstructor = getModalConstructor();
          obj = new modalConstructor(options, options.trigger);

          {

              mask = {
                  resize: function resize$$1() {
                      obj.resize();
                  },
                  open: function open() {
                      obj.open();
                  },
                  close: function close(index, noAnim) {
                      obj.close(index, noAnim);
                  },
                  on: function on() {
                      obj.on();
                  },
                  off: function off() {
                      obj.off();
                  },
                  destroy: function destroy() {
                      obj.destroy();
                  }
              };
              options.trigger.data('model', mask);
          }
          var optionModel = options.model;

          optionModel.on(isTransitionEndSupported, function (e) {
              if (e.target == optionModel[0]) {
                  if (optionModel.hasClass(obj.options.close.anim)) {
                      obj.closeTransEnd_cb.call(obj);
                  }
              }
          });
      }
      return options.trigger.data('model') || obj;
  };


  /*End of lightBox*/

  /** isTransitionSuppported referred from model */

  var pluginName$1 = 'drawer';
  var default_opt$1 = {
      fixed: true,
      "dir": "right",
      "open": {
          "anim": "sideIn"
      },
      "close": {
          "anim": "sideOut" /*Empty class required*/
      } };
  var drawerModal = null;

  /**
   * [description]
   * @return {[type]} [drawerModal singleton object]
   */
  var getModalConstructor$1 = function getModalConstructor() {
      if (!drawerModal) {
          drawerModal = getNewModel(pluginName$1);

          /*drawerModal.prototype.resize = function() {
               this.lt.cont.css({
                  width: 'auto',
                  height: 'auto'
              })
               var totalH = $(window).height(),
                  totalW = $(window).width();
               this.lt.cont.css({
                  width: totalW + 'px',
                  height: totalH + 'px'
              });
           };*/
      }

      return drawerModal;
  };

  /** Wrapped to handle drawer open and close */
  var drawer = function drawer(options) {
      var obj = null,
          mask = null;

      options = _extends({}, default_opt$1, options);

      if (util.is_options_valid(options)) {
          var modalConstructor = getModalConstructor$1();
          obj = new modalConstructor(options, options.trigger);

          {
              mask = {
                  resize: function resize$$1() {
                      obj.resize();
                  },
                  open: function open() {
                      obj.open();
                  },
                  close: function close(index, noAnim) {
                      obj.close(index, noAnim);
                  },
                  on: function on() {
                      obj.on();
                  },
                  off: function off() {
                      obj.off();
                  },
                  destroy: function destroy() {
                      obj.destroy();
                  }
              };
              options.trigger.data("model", mask);
          }
          var optionModel = options.model;

          optionModel.on(isTransitionEndSupported, function (e) {
              if (e.target == optionModel[0]) {
                  if (optionModel.hasClass(obj.options.close.anim)) {
                      obj.closeTransEnd_cb.call(obj);
                  }
              }
          });
      }
      return options.trigger.data('model') || obj;
  };

  exports.lightBox = lightBox;
  exports.drawer = drawer;
  exports.modelSetup = modelSetup;
  exports.cleanQueryString = cleanQueryString;

  Object.defineProperty(exports, '__esModule', { value: true });

});
