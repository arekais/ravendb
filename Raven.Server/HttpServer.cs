using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using log4net;
using Newtonsoft.Json;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Server.Responders;

namespace Raven.Server
{
	public class HttpServer : IDisposable
	{
		private readonly RavenConfiguration configuration;
		private readonly HttpListener listener;

		private readonly ILog logger = LogManager.GetLogger(typeof (HttpServer));
		private readonly RequestResponder[] requestResponders;
		private int reqNum;

		public HttpServer(
			RavenConfiguration configuration,
			IEnumerable<RequestResponder> requestResponders)
		{
			this.configuration = configuration;
			this.requestResponders = requestResponders.ToArray();
			listener = new HttpListener();
			listener.Prefixes.Add("http://+:" + configuration.Port + "/" + configuration.VirtualDirectory);
			switch (configuration.AnonymousUserAccessMode)
			{
				case AnonymousUserAccessMode.None:
					listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication;
					break;
				default:
					listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
						AuthenticationSchemes.Anonymous;
					break;
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			listener.Stop();
		}

		#endregion

		public void Start()
		{
			listener.Start();
			listener.BeginGetContext(GetContext, null);
		}

		private void GetContext(IAsyncResult ar)
		{
			HttpListenerContext ctx;
			try
			{
				ctx = listener.EndGetContext(ar);
				//setup waiting for the next request
				listener.BeginGetContext(GetContext, null);
			}
			catch (HttpListenerException)
			{
				// can't get current request / end new one, probably
				// listner shutdown
				return;
			}

			var curReq = Interlocked.Increment(ref reqNum);
			try
			{
				logger.DebugFormat("Request #{0}: {1} {2}",
				                   curReq,
				                   ctx.Request.HttpMethod,
				                   ctx.Request.Url.PathAndQuery
					);
				var sw = Stopwatch.StartNew();
				HandleRequest(ctx);

				logger.DebugFormat("Request #{0}: {1} {2} - {3}",
				                   curReq, ctx.Request.HttpMethod, ctx.Request.Url.PathAndQuery, sw.Elapsed);
			}
			catch (Exception e)
			{
				HandleException(ctx, e);
				logger.WarnFormat(e, "Error on request #{0}", curReq);
			}
			finally
			{
				try
				{
					ctx.Response.OutputStream.Flush();
					ctx.Response.Close();
				}
				catch
				{
				}
			}
		}

		private void HandleException(HttpListenerContext ctx, Exception e)
		{
			try
			{
				if (e is BadRequestException)
					HandleBadRequest(ctx, (BadRequestException)e);
				else if (e is ConcurrencyException)
					HandleConcurrencyException(ctx, (ConcurrencyException)e);
				else if (e is IndexDisabledException)
					HandleIndexDisabledException(ctx, (IndexDisabledException)e);
				else if (e is TooBusyException)
					HandleTooBudyException(ctx);
				else
					HandleGenericException(ctx, e);
			}
			catch (Exception)
			{
				logger.Error("Failed to properly handle error, further error handling is ignored", e);
			}
		}

		private static void HandleTooBudyException(HttpListenerContext ctx)
		{
			ctx.Response.StatusCode = 503;
			ctx.Response.StatusDescription = "Service Unavailable";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = "The server is too busy, could not acquire transactional access"
			});
		}

		private static void HandleIndexDisabledException(HttpListenerContext ctx, IndexDisabledException e)
		{
			ctx.Response.StatusCode = 503;
			ctx.Response.StatusDescription = "Service Unavailable";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = e.Information.GetErrorMessage(),
				Index = e.Information.Name,
			});
		}

		private static void HandleGenericException(HttpListenerContext ctx, Exception e)
		{
			ctx.Response.StatusCode = 500;
			ctx.Response.StatusDescription = "Internal Server Error";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = e.ToString()
			});
		}

		private static void HandleBadRequest(HttpListenerContext ctx, BadRequestException e)
		{
			ctx.SetStatusToBadRequest();
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				e.Message,
				Error = e.Message
			});
		}

		private static void HandleConcurrencyException(HttpListenerContext ctx, ConcurrencyException e)
		{
			ctx.Response.StatusCode = 409;
			ctx.Response.StatusDescription = "Conflict";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				e.ActualETag,
				e.ExpectedETag,
				Error = e.Message
			});
		}

		private static void SerializeError(HttpListenerContext ctx, object error)
		{
			using (var sw = new StreamWriter(ctx.Response.OutputStream))
			{
				new JsonSerializer().Serialize(new JsonTextWriter(sw)
				{
					Formatting = Formatting.Indented,
				}, error);
			}
		}

		private void HandleRequest(HttpListenerContext ctx)
		{
			if (AssertSecurityRights(ctx) == false)
				return;

			foreach (var requestResponder in requestResponders)
			{
				if (requestResponder.WillRespond(ctx))
				{
					requestResponder.Respond(ctx);
					return;
				}
			}
			ctx.SetStatusToBadRequest();
			ctx.Write(
				@"
<html>
    <body>
        <h1>Could not figure out what to do</h1>
        <p>Your request didn't match anything that Raven knows to do, sorry...</p>
    </body>
</html>
");
		}

		private bool AssertSecurityRights(HttpListenerContext ctx)
		{
			if (configuration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				(ctx.User == null || ctx.User.Identity == null || ctx.User.Identity.IsAuthenticated == false) &&
					ctx.Request.HttpMethod != "GET"
				)
			{
				ctx.SetStatusToUnauthorized();
				return false;
			}
			return true;
		}
	}
}