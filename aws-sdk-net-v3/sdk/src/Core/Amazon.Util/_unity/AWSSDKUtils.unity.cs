﻿/*******************************************************************************
 *  Copyright 2008-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *  this file except in compliance with the License. A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file.
 *  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 * *****************************************************************************
 *    __  _    _  ___
 *   (  )( \/\/ )/ __)
 *   /__\ \    / \__ \
 *  (_)(_) \/\/  (___/
 *
 *  AWS SDK for .NET
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;

using Microsoft.Win32;
using System.Globalization;

namespace Amazon.Util
{
    public static partial class AWSSDKUtils
    {
        public static void ForceCanonicalPathAndQuery(Uri uri)
        {
            try
            {
                FieldInfo flagsFieldInfo = typeof(Uri).GetField("m_Flags", BindingFlags.Instance | BindingFlags.NonPublic);
                ulong flags = (ulong)flagsFieldInfo.GetValue(uri);
                flags &= ~((ulong)0x30); // Flags.PathNotCanonical|Flags.QueryNotCanonical
                flagsFieldInfo.SetValue(uri, flags);
            }
            catch { }
        }

        static readonly object _preserveStackTraceLookupLock = new object();
        static bool _preserveStackTraceLookup = false;
        static MethodInfo _preserveStackTrace;
		
        /// <summary>
        /// This method is used preserve the stacktrace used from clients that support async calls.  This 
        /// make sure that exceptions thrown during EndXXX methods has the orignal stacktrace that happen 
        /// in the background thread.
        /// </summary>
        /// <param name="exception"></param>
        public static void PreserveStackTrace(Exception exception)
        {
            if (!_preserveStackTraceLookup)
            {
                lock (_preserveStackTraceLookupLock)
                {
                    _preserveStackTraceLookup = true;
                    try
                    {
                        _preserveStackTrace = typeof(Exception).GetMethod("InternalPreserveStackTrace",
                            BindingFlags.Instance | BindingFlags.NonPublic);
                    }
                    catch { }
                }
            }

            if (_preserveStackTrace != null)
            {
                _preserveStackTrace.Invoke(exception, null);
            }
        }

        private const int _defaultDefaultConnectionLimit = 2;
        internal static int GetConnectionLimit(int? clientConfigValue)
        {
            // Connection limit has been explicitly set on the client.
            if (clientConfigValue.HasValue)
                return clientConfigValue.Value;

            return AWSSDKUtils.DefaultConnectionLimit;
        }

        private const int _defaultMaxIdleTime = 100 * 1000;
        internal static int GetMaxIdleTime(int? clientConfigValue)
        {
            // MaxIdleTime has been explicitly set on the client.
            if (clientConfigValue.HasValue)
                return clientConfigValue.Value;
			
            return AWSSDKUtils.DefaultMaxIdleTime;
        }

        public static void Sleep(int ms)
        {
            System.Threading.Thread.Sleep(ms);
        }

        /// <summary>
        /// Determines if Unity scripting backend is IL2CPP.
        /// </summary>
        /// <returns><c>true</c>If scripting backend is IL2CPP; otherwise, <c>false</c>.</returns>
        internal static bool IsIL2CPP
        {
            get{
                Type type = Type.GetType("Mono.Runtime");
                if (type != null)
                {                                          
                    MethodInfo displayName = type.GetMethod("GetDisplayName", BindingFlags.NonPublic | BindingFlags.Static); 
                    if (displayName != null)
                    {
                        string name = null;
                        try
                        {
                            name = displayName.Invoke(null, null).ToString();
                        }
                        catch(Exception)
                        {
                            return false;
                        }
                
                        if(name != null && name.ToUpper().Contains("IL2CPP"))
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
        }
    }
}
