/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V.
 * Licensed under the Elastic License 2.0.
 */
package org.elasticsearch.paimon;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.io.FileArchiveDataProvider;
import org.elasticsearch.eslib.io.OSSArchiveDataProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Opens archive range readers without putting credentials in index settings. */
final class MountArchiveProviderFactory {

    private MountArchiveProviderFactory() {}

    static ArchiveDataProvider open(
            ShardMountSpec spec,
            Settings nodeSettings,
            SecureString ossAccessKeySecret,
            Environment environment)
            throws IOException {
        // A Windows drive letter looks like a URI scheme ("D:"). Resolve it as a path before
        // invoking URI parsing, which also avoids rejecting the backslashes in a native path.
        if (isWindowsDrivePath(spec.archiveLocation)) {
            return openLocal(resolveRepoPath(spec.archiveLocation, environment), spec);
        }
        URI uri = parseLocation(spec.archiveLocation);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.equalsIgnoreCase("file")) {
            Path path =
                    scheme == null
                            ? resolveRepoPath(spec.archiveLocation, environment)
                            : resolveRepoFileUri(uri, environment);
            return openLocal(path, spec);
        }
        if (scheme.equalsIgnoreCase("oss")) {
            String endpoint = PaimonStorePlugin.OSS_ENDPOINT.get(nodeSettings);
            String accessKeyId = PaimonStorePlugin.OSS_ACCESS_KEY_ID.get(nodeSettings);
            if (ossAccessKeySecret == null || ossAccessKeySecret.length() == 0) {
                throw new IOException(
                        "Missing secure node setting paimon.oss.access_key_secret for "
                                + spec.archiveLocation);
            }
            String bucket = uri.getHost();
            String path = uri.getPath();
            String key = path == null ? "" : path.replaceFirst("^/+", "");
            return new OSSArchiveDataProvider(
                    endpoint, accessKeyId, ossAccessKeySecret.toString(), bucket, key);
        }
        throw new IOException(
                "Unsupported mounted archive scheme '"
                        + scheme
                        + "' in "
                        + spec.archiveLocation
                        + "; supported schemes are file and oss");
    }

    private static URI parseLocation(String location) throws IOException {
        try {
            return new URI(location);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid mounted archive location: " + location, e);
        }
    }

    private static boolean isWindowsDrivePath(String location) {
        return location.matches("^[A-Za-z]:[\\\\/].*");
    }

    private static Path resolveRepoPath(String location, Environment environment)
            throws IOException {
        Path path = environment.resolveRepoDir(location);
        if (path == null) {
            throw new IOException(
                    "Local Paimon archive is outside every path.repo root: " + location);
        }
        return path;
    }

    private static Path resolveRepoFileUri(URI uri, Environment environment) throws IOException {
        try {
            java.net.URL resolved = environment.resolveRepoURL(uri.toURL());
            if (resolved == null) {
                throw new IOException(
                        "Local Paimon archive is outside every path.repo root: " + uri);
            }
            return Paths.get(resolved.toURI());
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new IOException("Invalid local Paimon archive URI: " + uri, e);
        }
    }

    private static ArchiveDataProvider openLocal(Path path, ShardMountSpec spec)
            throws IOException {
        FileArchiveDataProvider provider = new FileArchiveDataProvider(path);
        if (provider.length() != spec.archiveLength) {
            provider.close();
            throw new IOException(
                    "Mounted archive length changed for "
                            + spec.archiveLocation
                            + ": manifest="
                            + spec.archiveLength
                            + ", actual="
                            + java.nio.file.Files.size(path));
        }
        return provider;
    }
}
